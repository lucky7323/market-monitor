"""Data models for the pipeline."""
from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum


class SourceType(str, Enum):
    """Source type enumeration."""
    REST = "rest"
    CSV = "csv"
    WEBSOCKET = "websocket"
    GRAPHQL = "graphql"
    FILE = "file"


class Record(BaseModel):
    """Normalized data record."""
    source: str
    timestamp: datetime
    value: Any
    unit: Optional[str] = None
    
    class Config:
        """Pydantic config."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SourceConfig(BaseModel):
    """Configuration for a data source."""
    name: str
    type: SourceType
    url: Optional[str] = None
    path: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    timeout: float = Field(default=10.0, gt=0)
    retry_count: int = Field(default=3, ge=0)
    field_mapping: Optional[Dict[str, str]] = None
    
    # Source-specific configs
    graphql_query: Optional[str] = None
    csv_delimiter: str = ","
    websocket_subscription: Optional[str] = None


class PipelineStats(BaseModel):
    """Pipeline execution statistics."""
    total_sources: int
    successful_sources: int
    failed_sources: List[str]
    total_records: int
    execution_time_seconds: float
    records_per_second: float


class PipelineResult(BaseModel):
    """Pipeline execution result."""
    records: List[Record]
    stats: PipelineStats
    
    class Config:
        """Pydantic config."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }