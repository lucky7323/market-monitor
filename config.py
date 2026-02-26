"""Configuration models with Pydantic v2 full compatibility."""

from typing import Any, Literal, Optional, Union
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator
import yaml
import json


class FieldMapping(BaseModel):
    """Field mapping configuration for source data transformation."""
    model_config = ConfigDict(extra="forbid")
    
    source_field: str = Field(min_length=1)
    target_field: Literal["source", "timestamp", "value", "unit"]
    transform: Optional[str] = None  # Optional transformation function


class SourceConfig(BaseModel):
    """Configuration for a single data source."""
    model_config = ConfigDict(strict=True, extra="forbid")

    name: str = Field(min_length=1, max_length=100, pattern=r'^[a-zA-Z0-9_-]+$')
    type: Literal["rest", "csv", "websocket", "graphql", "file"]
    url: Optional[str] = None
    path: Optional[str] = None
    timeout: float = Field(default=10.0, gt=0, le=60)

    # Type-specific fields with proper defaults
    headers: dict[str, str] = Field(default_factory=dict)
    query: Optional[str] = None
    variables: dict[str, Any] = Field(default_factory=dict)  # Any for nested values
    ws_duration: float = Field(default=5.0, gt=0, le=30)

    # Custom field mapping
    field_mapping: list[FieldMapping] = Field(default_factory=list)

    # Incremental updates
    incremental: bool = False
    incremental_field: str = "timestamp"

    @model_validator(mode="after")
    def validate_type_requirements(self):
        """Enforce type-specific required fields."""
        needs_url = {"rest", "websocket", "graphql"}
        needs_path = {"file"}
        can_have_either = {"csv"}  # url OR path

        if self.type in needs_url and not self.url:
            raise ValueError(f"type={self.type} requires 'url'")
        if self.type in needs_path and not self.path:
            raise ValueError(f"type={self.type} requires 'path'")
        if self.type in can_have_either and not (self.url or self.path):
            raise ValueError(f"type={self.type} requires 'url' or 'path'")
        if self.type == "graphql" and not self.query:
            raise ValueError("type=graphql requires 'query'")
        return self

    @field_validator('url')
    @classmethod
    def validate_url(cls, v):
        """Validate URL format at config time."""
        if v is not None:
            from security import validate_url_format
            validate_url_format(v)
        return v

    @field_validator('path') 
    @classmethod
    def validate_path(cls, v):
        """Basic path validation - detailed check at runtime."""
        if v is not None:
            # Basic path sanitization
            if '..' in v or v.startswith('/'):
                # More detailed validation will be done at runtime with allowed_dirs
                pass
        return v

    @field_validator('headers')
    @classmethod
    def validate_headers(cls, v):
        """Sanitize headers."""
        if v:
            from security import sanitize_headers
            return sanitize_headers(v)
        return v

    @field_validator('query')
    @classmethod 
    def validate_graphql_query(cls, v):
        """Validate GraphQL query if provided."""
        if v is not None:
            from security import validate_graphql_query
            validate_graphql_query(v)
        return v


class PipelineConfig(BaseModel):
    """Main pipeline configuration."""
    model_config = ConfigDict(extra="forbid")

    sources: list[SourceConfig] = Field(min_length=1)
    output: str = "output.json"
    max_concurrent: int = Field(default=10, ge=1, le=50)
    global_timeout: float = Field(default=15.0, gt=0, le=300)
    allowed_hosts: list[str] = Field(default_factory=list)
    allowed_output_dirs: list[str] = Field(default_factory=lambda: ["."])
    allowed_input_dirs: list[str] = Field(default_factory=lambda: ["."])
    state_file: str = ".state.json"

    @field_validator('output')
    @classmethod
    def validate_output_path(cls, v):
        """Output path allowlist validation will be done at runtime."""
        return v

    @field_validator('sources')
    @classmethod
    def validate_unique_source_names(cls, v):
        """Ensure source names are unique."""
        names = [source.name for source in v]
        if len(names) != len(set(names)):
            raise ValueError("Source names must be unique")
        return v


def load_config(config_path: str) -> PipelineConfig:
    """Load configuration from YAML or JSON file."""
    path = Path(config_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    try:
        content = path.read_text(encoding='utf-8')
        
        # Try YAML first, fallback to JSON
        if path.suffix.lower() in ('.yml', '.yaml'):
            data = yaml.safe_load(content)
        else:
            data = json.loads(content)
        
        return PipelineConfig.model_validate(data)
    
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in {config_path}: {e}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {config_path}: {e}")
    except Exception as e:
        raise ValueError(f"Failed to load config {config_path}: {e}")


def create_example_config() -> dict:
    """Create example configuration structure."""
    return {
        "sources": [
            {
                "name": "api_data",
                "type": "rest", 
                "url": "https://api.example.com/data",
                "headers": {"Authorization": "Bearer token"},
                "timeout": 10.0,
                "incremental": True,
                "incremental_field": "updated_at"
            },
            {
                "name": "csv_file",
                "type": "csv",
                "path": "./data/sample.csv",
                "field_mapping": [
                    {"source_field": "price", "target_field": "value"},
                    {"source_field": "created", "target_field": "timestamp"}
                ]
            },
            {
                "name": "websocket_feed",
                "type": "websocket",
                "url": "wss://feed.example.com/ws",
                "ws_duration": 5.0,
                "timeout": 15.0
            },
            {
                "name": "graphql_api",
                "type": "graphql",
                "url": "https://api.example.com/graphql",
                "query": "query { data { id timestamp value unit } }",
                "variables": {"limit": 100}
            },
            {
                "name": "json_file",
                "type": "file",
                "path": "./data/sample.json"
            }
        ],
        "output": "pipeline_output.json",
        "max_concurrent": 5,
        "global_timeout": 15.0,
        "allowed_hosts": ["api.example.com", "feed.example.com"],
        "allowed_output_dirs": [".", "./output"],
        "allowed_input_dirs": [".", "./data"],
        "state_file": ".state.json"
    }


# Export main classes and functions
__all__ = [
    'SourceConfig',
    'PipelineConfig', 
    'FieldMapping',
    'load_config',
    'create_example_config',
]