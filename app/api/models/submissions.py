from typing import Optional
from pydantic import BaseModel


class StepRetryRequest(BaseModel):
    session_id: str
    submission_id: str
    tenant_code: str


class BulkRetryRequest(BaseModel):
    tenant_code: Optional[str] = None  # None = retry across all tenants


class StepRetryResponse(BaseModel):
    workflow_id: str
    submission_id: str
    session_id: str
    message: str


class BulkRetryResponse(BaseModel):
    workflow_id: str
    message: str
