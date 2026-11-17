from typing import Any, Dict, Optional
from pydantic import BaseModel


class UploadResponse(BaseModel):
    message: str
    id: int
    status: str
    # feedback only, and only when the persona's config sets expect_stages: report-only COUNTS (distinct unexpected
    # stage values and the sessions that carry them; never a value from the file), see feedback_uploads.report_unexpected_stages. Omitted from the response otherwise.
    stage_report: Optional[Dict[str, Any]] = None


class CsvUploadStatus(BaseModel):
    id: int
    status: str
    report_type: str
    cloud_storage_path: str
    created_at: Optional[str] = None
