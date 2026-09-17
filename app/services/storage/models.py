from dataclasses import dataclass
from enum import Enum

class AccessMode(str, Enum):
    PRIVATE = "private"
    PUBLIC  = "public"

@dataclass(frozen=True)
class StoredObject:
    provider:     str
    bucket:       str
    key:          str
    access_mode:  AccessMode   = AccessMode.PRIVATE
    content_type: str | None   = None
    version_id:   str | None   = None
