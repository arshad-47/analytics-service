# syntax=docker/dockerfile:1
FROM python:3.10-slim

# libgl1/libglib2.0-0: needed by opencv-python (pulled in transitively via `deface`).
# confluent-kafka needs no extra apt packages — manylinux wheels cover this base image.
RUN apt-get update && apt-get install -y --no-install-recommends \
      libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/opt/model-cache/huggingface

WORKDIR /app

COPY requirements-prod.txt .
RUN pip install -r requirements-prod.txt \
      --extra-index-url https://download.pytorch.org/whl/cpu

# Bake the embedding model so the running container never needs internet access
# for it (HF_HOME set above makes the cache path stable between build and run).
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

# deface's centerface.onnx ships inside the pip package (not a runtime download) —
# this just fails the build loudly if a future deface release changes that.
RUN python -c "import os, deface.centerface as c; assert os.path.exists(c.default_onnx_path)"

COPY app/ app/
COPY main.py schema.sql seed_prompts.sql seed_themes.sql seed_config.sql run_all.sh ./
# The operator tools the README and the app's own messages tell people to run inside the container
# (python tools/feedback_config.py ..., questionnaire_versions.py, run_feedback_extraction.py). split_mitra_export.py
# is NOT here: it prepares a CSV on the workstation that holds the export. tests/test_feedback_upgrade.py derives the
# required list from the repo, so a tool named in a message or the README must be added to this line.
COPY tools/feedback_config.py tools/questionnaire_versions.py tools/run_feedback_extraction.py tools/
RUN chmod +x run_all.sh

RUN mkdir -p logs downloads

ENTRYPOINT ["python", "main.py"]
CMD ["--mode", "all"]
