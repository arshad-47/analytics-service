import json
import os
from typing import Dict, Any, List
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="localhost:9092")
    KAFKA_TOPIC_INGESTION: str = Field(default="analytics.ingestion.raw")
    KAFKA_TOPIC_INGESTION_DLQ: str = Field(default="analytics.ingestion.raw.dlq")
    KAFKA_GROUP_ID: str = Field(default="analytics-ingestion-group")

    # Database Configuration
    DATABASE_URL: str = Field(default="postgresql://postgres:postgres@localhost:5432/temporal")
    # asyncpg connection pool bounds — this is the real concurrency ceiling for
    # DB-touching activities (insert/update submission, llm_logs, etc). Sized too
    # small and high-concurrency batches (e.g. 200+ simultaneous real-time
    # submissions) stall almost entirely on pool.acquire() rather than failing
    # fast, since activities queue for a connection instead of erroring out.
    DATABASE_POOL_MIN_SIZE: int = Field(default=2, gt=0)
    DATABASE_POOL_MAX_SIZE: int = Field(default=10, gt=0)

    # Orchestration Mode: 'real-time' or 'batch'
    PROCESSING_MODE: str = Field(default="real-time")
    BATCH_SCHEDULE_CRON: str = Field(default="0 20 * * *")
    # Max pending submissions fetched/fanned-out per chunk in BatchProcessingWorkflow —
    # keeps memory and concurrent child-workflow count bounded regardless of queue size.
    BATCH_SIZE: int = Field(default=100, gt=0)

    # Deployment environment — defaults to the safe option ('production') so a
    # missing/unset value never accidentally allows destructive operations like
    # RESET_DB. Must be explicitly set to 'development' to opt into those.
    ENVIRONMENT: str = Field(default="production")

    # Optional full schema reset on startup — only takes effect when
    # ENVIRONMENT=development (see db.py's initialize_schema), so a stray
    # RESET_DB=true in a production config can't wipe a live database.
    RESET_DB: bool = Field(default=False)

    # Temporal Configuration
    TEMPORAL_HOST: str = Field(default="localhost:7233")
    # Lets multiple environments (dev/qa) share one Temporal cluster safely.
    # Namespaces are Temporal's own multi-tenancy primitive — task queues and
    # workflow history are scoped per-namespace, so two environments in
    # different namespaces never see each other's work. Without this, every
    # Client.connect() call implicitly uses the "default" namespace, and if
    # dev and qa also share TEMPORAL_QUEUE, their workers compete for and can
    # execute each other's tasks — each looking up the resulting submission_id
    # in its OWN database, which fails (or worse, hits the wrong row).
    TEMPORAL_NAMESPACE: str = Field(default="default")
    TEMPORAL_QUEUE: str = Field(default="analytics-processing-queue")
    # Caps how many activities the worker runs simultaneously, regardless of how
    # many workflows are started/queued — this is what actually protects the DB
    # pool from an unpredictable event burst. Without a cap, every incoming
    # event immediately becomes a concurrent DB-touching activity (load-tested:
    # a 500-event burst instantly saturated a 100-connection Postgres instance).
    # Anything beyond this limit waits safely in Temporal's own task queue
    # instead of piling onto Postgres. Keep at or below DATABASE_POOL_MAX_SIZE.
    WORKER_MAX_CONCURRENT_ACTIVITIES: int = Field(default=40, gt=0)

    # Image processing (deface_blur_activity) concurrency — tune these against
    # actual server specs (CPU cores, available memory), not whatever machine
    # they were load-tested on. Download/upload are cheap I/O; face-blur spawns
    # a real subprocess with a fresh ONNX model load every call and is the
    # memory-expensive step — see deface_blur_activity.py for why these are
    # treated as separate concerns rather than one concurrency number.
    IMAGE_EXECUTOR_MAX_WORKERS: int = Field(default_factory=lambda: max(4, (os.cpu_count() or 4) * 2), gt=0)
    PER_SUBMISSION_IMAGE_CONCURRENCY: int = Field(default=3, gt=0)
    BLUR_CONCURRENCY_LIMIT: int = Field(default=2, gt=0)

    # API Authentication — single shared Bearer token, checked via
    # secrets.compare_digest in app/api/deps.py. Required (no default): the app
    # will not start without it, since this is an eagerly-evaluated singleton.
    AUTH_TOKEN: str = Field(description="Bearer token for API authentication. Must be set via environment variable.")

    # CSV Upload / Processing Configuration
    MAX_CSV_UPLOAD_BYTES: int = Field(default=10485760)  # 10MB
    CSV_BLOB_UPLOADS: str = Field(default="mitra_dashboard_api_output")
    # Expected CSV column headers per report type (JSON arrays of column names,
    # matched case-insensitively against the uploaded file's header row).
    STORY_CSV_COLUMN: str = Field(
        default='["id","Title","User name","Designation","Location","District","Organization","Report Created At","Objective","Challenges","Action Steps","Impact","Duration","Blurb","Images","Pdf","Transcript Link","Session ID"]'
    )
    DISCUSSION_CSV_COLUMN: str = Field(
        default='["id","Title","User name","User Location","District","Participant Count","Men","Women","Children","Date of Discussion","Organization","Challenges","Solutions","Author","Language","Report Created At","Transcript Link","Image Urls","PDF Urls","Session ID"]'
    )
    # Maps a discussion participant "role" to the CSV column name holding its
    # count (JSON object). See get_discussion_participants_map().
    DISCUSSION_PARTICIPANTS_MAP: str = Field(
        default='{"men": "Men", "women": "Women", "children": "Children", "teacher": "Teacher", "participant count": "Participant Count"}'
    )

    # LLM / OpenRouter Configuration
    OPENROUTER_API_KEY: str = Field(default="")
    OPENROUTER_MODEL: str = Field(default="nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free")
    OPENROUTER_BASE_URL: str = Field(default="https://openrouter.ai/api/v1")
    LLM_TEMPERATURE: float = Field(default=0.0)
    LLM_MAX_TOKENS: int = Field(default=2048)
    LLM_TIMEOUT_SECONDS: int = Field(default=60)

    # Story Rating Configuration
    MAX_PDF_TEXT_CHARS: int = Field(default=40000)

    # Logging Configuration
    LOG_DIR: str = Field(default="logs")
    LOG_LEVEL: str = Field(default="INFO")
    LOG_RETENTION_DAYS: int = Field(default=14, gt=0)

    # Scoped Ingestion Pipeline Configuration (JSON strings loaded from env)
    PROCESS_CONFIG_STORY: str = Field(
        default='[{"name": "pii_and_abusive_language_detection", "columns": ["objective"]}, {"name": "thematic_classification", "columns": ["objective"]}, {"name": "story_rating"}]'
    )
    PROCESS_CONFIG_DISCUSSION: str = Field(
        default='[{"name": "pii_and_abusive_language_detection", "columns": ["challenges"]}, {"name": "thematic_classification", "columns": ["challenges"]}]'
    )

    # Kafka ingestion-time field validation schema (JSON strings loaded from env).
    # One entry per (create/update/delete) event type: "required" is a list of
    # dot-notation paths (e.g. "tags.state", "data.pdfUrls.original") that must be
    # present and non-empty; "optional" is informational only. "update" additionally
    # sets newValuesNoEmpty so every key actually present in newValues must be non-empty.
    STORY_KAFKA_SCHEMA: str = Field(
        default='{"create": {"required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt", "tags.state", "tags.district", "tags.organization", "tags.programId", "tags.programName", "tags.leaderCategoryId", "tags.leaderCategoryName", "data.title", "data.designation", "data.submissionDate", "data.pdfUrls.original", "data.pdfUrls.masked", "data.transcriptLink", "data.challenges", "data.objective", "data.actionSteps", "data.impact", "data.duration", "data.blurb", "data.content"], "optional": ["data.imageUrls"]}, "update": {"required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt"], "newValuesNoEmpty": true}, "delete": {"required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt"]}}'
    )
    DISCUSSION_KAFKA_SCHEMA: str = Field(
        default='{"create": {"required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt", "tags.state", "tags.district", "tags.organization", "tags.programId", "tags.programName", "tags.leaderCategoryId", "tags.leaderCategoryName", "data.title", "data.designation", "data.submissionDate", "data.discussionDate", "data.pdfUrls.original", "data.pdfUrls.masked", "data.transcriptLink", "data.challenges", "data.solutions", "data.participantsData"], "optional": ["data.author", "data.language", "data.imageUrls"]}, "update": {"required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt"], "newValuesNoEmpty": true}, "delete": {"required": ["submissionId", "submissionType", "sessionId", "tenantCode", "eventType", "eventPublishedAt"]}}'
    )

    # Thematic Classification Configuration
    MINIMUM_THEME_WORD_COUNT: int = Field(default=5)
    EMBEDDING_MODEL_NAME: str = Field(default="all-MiniLM-L6-v2")
    SIMILARITY_SCORE_THRESHOLD: float = Field(default=0.65)
    LLM_CONFIDENCE_SCORE_THRESHOLD: float = Field(default=0.8)

    # GCP Credentials
    TYPE: str = Field(default="service_account")
    PROJECT_ID: str = Field(default="")
    PRIVATE_KEY_ID: str = Field(default="")
    PRIVATE_KEY: str = Field(default="")
    CLIENT_EMAIL: str = Field(default="")
    CLIENT_ID: str = Field(default="")
    AUTH_URI: str = Field(default="")
    TOKEN_URI: str = Field(default="")
    AUTH_PROVIDER_X509_CERT_URL: str = Field(default="")
    CLIENT_X509_CERT_URL: str = Field(default="")
    UNIVERSE_DOMAIN: str = Field(default="googleapis.com")
    BUCKET_NAME: str = Field(default="")
    STORY_BLOB: str = Field(default="")
    DISCUSSION_BLOB: str = Field(default="")
    MEDIA_BASE_URL: str = Field(default="")
    
    # Generic Storage Configuration
    STORAGE_PROVIDER: str = Field(default="gcp") # gcp | aws | oci | azure | s3-compatible
    # Two separate buckets: one public (blurred images), one private (internal CSVs)
    STORAGE_PUBLIC_BUCKET: str = Field(default="")   # bucket for AccessMode.PUBLIC objects (blurred images)
    STORAGE_PRIVATE_BUCKET: str = Field(default="")  # bucket for AccessMode.PRIVATE objects (CSVs)
    STORAGE_REGION: str = Field(default="")
    STORAGE_ENDPOINT_URL: str | None = Field(default=None) # S3-compatible providers only
    STORAGE_STORY_PREFIX: str = Field(default="story_blurred_image")
    STORAGE_DISCUSSION_PREFIX: str = Field(default="discussion_blurred_image")
    STORAGE_CSV_PREFIX: str = Field(default="mitra_dashboard_api_output")
    STORAGE_SIGNED_URL_TTL_SECONDS: int = Field(default=3600)
    STORAGE_CONNECT_TIMEOUT_SECONDS: int = Field(default=10)
    STORAGE_READ_TIMEOUT_SECONDS: int = Field(default=60)
    STORAGE_MAX_RETRIES: int = Field(default=3)

    # AWS S3 Configuration
    AWS_ACCESS_KEY_ID: str = Field(default="")
    AWS_SECRET_ACCESS_KEY: str = Field(default="")
    AWS_SESSION_TOKEN: str = Field(default="")
    AWS_DEFAULT_REGION: str = Field(default="")

    # OCI Configuration
    OCI_NAMESPACE: str = Field(default="")
    OCI_CONFIG_FILE: str = Field(default="~/.oci/config")
    OCI_CONFIG_PROFILE: str = Field(default="DEFAULT")
    OCI_REGION: str = Field(default="")

    # Azure Configuration
    AZURE_STORAGE_ACCOUNT_NAME: str = Field(default="")
    AZURE_STORAGE_CONNECTION_STRING: str = Field(default="")
    AZURE_STORAGE_CONTAINER: str = Field(default="")
    AZURE_CLIENT_ID: str = Field(default="")
    AZURE_CLIENT_SECRET: str = Field(default="")
    AZURE_TENANT_ID: str = Field(default="")

    # Image Blur CPU Throttling
    # Downscale resolution for face-detection neural network (WxH).
    # Only affects detection speed — blur is applied to the original full-res image.
    DEFACE_SCALE: str = Field(default="640x360")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    @field_validator("DATABASE_URL")
    @classmethod
    def validate_database_url(cls, v: str) -> str:
        # asyncpg requires postgresql:// or postgres:// scheme
        if v.startswith("postgresql+asyncpg://"):
            return v.replace("postgresql+asyncpg://", "postgresql://")
        return v

    @model_validator(mode="after")
    def validate_pool_bounds(self) -> "Settings":
        # Without this, an inconsistent env config loads without error and only
        # fails later at asyncpg.create_pool() with a generic ValueError that
        # doesn't name the misconfigured variables.
        if self.DATABASE_POOL_MIN_SIZE > self.DATABASE_POOL_MAX_SIZE:
            raise ValueError(
                f"DATABASE_POOL_MIN_SIZE ({self.DATABASE_POOL_MIN_SIZE}) must not exceed "
                f"DATABASE_POOL_MAX_SIZE ({self.DATABASE_POOL_MAX_SIZE})."
            )
        return self

    @field_validator("PROCESS_CONFIG_STORY", "PROCESS_CONFIG_DISCUSSION")
    @classmethod
    def validate_process_config_json(cls, v: str, info) -> str:
        try:
            parsed = json.loads(v)
            if not isinstance(parsed, list):
                raise ValueError(f"{info.field_name} must be a valid JSON array of process steps.")
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON configuration for {info.field_name}: {e}") from e
        return v

    @field_validator("LOG_DIR")
    @classmethod
    def validate_log_dir(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("LOG_DIR must not be blank.")
        return v

    @field_validator("LOG_LEVEL")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        level = v.strip().upper()
        if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(
                f"LOG_LEVEL must be one of DEBUG, INFO, WARNING, ERROR, CRITICAL; got {v!r}."
            )
        return level

    @field_validator("STORY_CSV_COLUMN", "DISCUSSION_CSV_COLUMN")
    @classmethod
    def validate_csv_column_json(cls, v: str, info) -> str:
        try:
            parsed = json.loads(v)
            if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
                raise ValueError(f"{info.field_name} must be a JSON array of column-name strings.")
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON configuration for {info.field_name}: {e}") from e
        return v

    @field_validator("DISCUSSION_PARTICIPANTS_MAP")
    @classmethod
    def validate_participants_map_json(cls, v: str) -> str:
        if not v or not v.strip():
            return v
        try:
            parsed = json.loads(v)
            if not isinstance(parsed, dict):
                raise ValueError("DISCUSSION_PARTICIPANTS_MAP must be a JSON object mapping role names to CSV column names.")
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON configuration for DISCUSSION_PARTICIPANTS_MAP: {e}") from e
        return v

    @field_validator("STORY_KAFKA_SCHEMA", "DISCUSSION_KAFKA_SCHEMA")
    @classmethod
    def validate_kafka_ingestion_schema_json(cls, v: str, info) -> str:
        try:
            parsed = json.loads(v)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(f"Invalid JSON configuration for {info.field_name}: {e}") from e

        if not isinstance(parsed, dict) or not {"create", "update", "delete"} <= parsed.keys():
            raise ValueError(
                f"{info.field_name} must be a JSON object with 'create', 'update', and 'delete' keys."
            )

        # consumer.py's _validate_ingestion_schema unconditionally does
        # event_schema.get("required", []) on each section, so a malformed section
        # (e.g. a string instead of an object) must be rejected here rather than
        # crashing the consumer at message-processing time.
        for section_name in ("create", "update", "delete"):
            section = parsed[section_name]
            if not isinstance(section, dict):
                raise ValueError(
                    f"{info.field_name}.{section_name} must be a JSON object, got {type(section).__name__}."
                )
            for list_field in ("required", "optional"):
                if list_field in section:
                    field_value = section[list_field]
                    if not isinstance(field_value, list) or not all(isinstance(item, str) for item in field_value):
                        raise ValueError(
                            f"{info.field_name}.{section_name}.{list_field} must be a list of strings."
                        )
            if "newValuesNoEmpty" in section and not isinstance(section["newValuesNoEmpty"], bool):
                raise ValueError(
                    f"{info.field_name}.{section_name}.newValuesNoEmpty must be a boolean."
                )

        return v

    def get_process_config(self, submission_type: str) -> List[Dict[str, Any]]:
        """
        Dynamically returns the process list configuration based on submission type.
        Raises ValueError if JSON parsing fails or if config is not a list.
        """
        normalized_type = submission_type.lower().strip()
        if "story" in normalized_type:
            raw_config = self.PROCESS_CONFIG_STORY
        elif "discussion" in normalized_type:
            raw_config = self.PROCESS_CONFIG_DISCUSSION
        else:
            # Fallback/Default config for unknown submission type
            return []

        try:
            parsed = json.loads(raw_config)
            if not isinstance(parsed, list):
                raise ValueError(
                    f"Process configuration for submission type {submission_type!r} must be a list, got {type(parsed).__name__}"
                )
            return parsed
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(
                f"Failed to parse process configuration JSON for submission type {submission_type!r}: {e}"
            ) from e

    def get_kafka_ingestion_schema(self, submission_type: str) -> Dict[str, Any]:
        """
        Dynamically returns the ingestion-time required-fields schema for the given
        submission type. Raises ValueError if JSON parsing fails, if the config is
        not a dict, or if the submission type doesn't match 'story' or 'discussion'.
        """
        normalized_type = submission_type.lower().strip() if isinstance(submission_type, str) else ""
        if "story" in normalized_type:
            raw_schema = self.STORY_KAFKA_SCHEMA
        elif "discussion" in normalized_type:
            raw_schema = self.DISCUSSION_KAFKA_SCHEMA
        else:
            raise ValueError(f"No Kafka ingestion schema defined for submission type {submission_type!r}")

        try:
            parsed = json.loads(raw_schema)
            if not isinstance(parsed, dict):
                raise ValueError(
                    f"Kafka ingestion schema for submission type {submission_type!r} must be a dict, got {type(parsed).__name__}"
                )
            return parsed
        except (json.JSONDecodeError, TypeError) as e:
            raise ValueError(
                f"Failed to parse Kafka ingestion schema JSON for submission type {submission_type!r}: {e}"
            ) from e

    def get_discussion_participants_map(self) -> Dict[str, str]:
        """
        Dynamically returns the participant role-to-column mapping dictionary.
        Falls back to empty dict if empty/invalid, or default if parsing fails.
        """
        raw_map = self.DISCUSSION_PARTICIPANTS_MAP
        if not raw_map or not str(raw_map).strip():
            return {}
        try:
            parsed = json.loads(raw_map)
            if isinstance(parsed, dict):
                return {str(k).strip(): str(v).strip() for k, v in parsed.items()}
            return {}
        except Exception:
            return {
                "men": "Men",
                "women": "Women",
                "children": "Children",
                "teacher": "Teacher",
                "participant count": "Participant Count"
            }

# Singleton instance
settings = Settings()
