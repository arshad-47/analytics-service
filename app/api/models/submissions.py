from typing import Optional
from pydantic import BaseModel, Field

from app.services.feedback_constants import SAFE_CODE_RE

# tenant_code of a request body ends up in log lines and workflow ids: letters, digits, '.', '_' and '-' only
# (no CR/LF or other control characters), at most 64 characters.


class StepRetryRequest(BaseModel):
    session_id: str
    submission_id: str
    tenant_code: str = Field(pattern=f"^{SAFE_CODE_RE}$")


class BulkRetryRequest(BaseModel):
    tenant_code: Optional[str] = Field(default=None, pattern=f"^{SAFE_CODE_RE}$")  # None = retry across all tenants


class StepRetryResponse(BaseModel):
    workflow_id: str
    submission_id: str
    session_id: str
    message: str


class BulkRetryResponse(BaseModel):
    workflow_id: str
    message: str
