"""Base classes for all data source fetchers."""

import time
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from config import SourceConfig
    from state import StateStore
    from security import SecureClientSession


@dataclass
class FetchResult:
    """Result of a fetch operation from a data source."""
    source_name: str
    records: list[dict[str, Any]] = field(default_factory=list)
    fetch_time_ms: float = 0.0
    error: Optional[str] = None
    is_partial: bool = False

    @property
    def success(self) -> bool:
        """True if fetch was successful (no error)."""
        return self.error is None
    
    @property
    def record_count(self) -> int:
        """Number of records fetched."""
        return len(self.records)


class BaseFetcher(ABC):
    """Abstract base class for all source fetchers."""

    def __init__(self, config: "SourceConfig", state_store: "StateStore"):
        """Initialize fetcher with configuration and state store."""
        self.config = config
        self.state = state_store

    @abstractmethod
    async def fetch(self, session: "SecureClientSession") -> FetchResult:
        """Fetch data from the source.
        
        Args:
            session: Secure HTTP session with SSRF protection
            
        Returns:
            FetchResult with fetched data and metadata
        """
        ...

    def get_incremental_filter(self) -> Optional[Any]:
        """Get the last processed value for incremental updates.
        
        Returns:
            Last watermark value or None if not using incremental updates
        """
        if not self.config.incremental:
            return None
        return self.state.get_watermark(self.config.name)

    async def _timed_fetch(self, session: "SecureClientSession") -> FetchResult:
        """Wrapper that measures fetch time."""
        start_time = time.time()
        try:
            result = await self.fetch(session)
            result.fetch_time_ms = (time.time() - start_time) * 1000
            return result
        except Exception as e:
            fetch_time = (time.time() - start_time) * 1000
            return FetchResult(
                source_name=self.config.name,
                records=[],
                fetch_time_ms=fetch_time,
                error=str(e),
                is_partial=False
            )

    def _apply_timeout(self, coro, timeout: float):
        """Apply timeout to a coroutine."""
        return asyncio.wait_for(coro, timeout=timeout)

    def _normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Apply field mapping to a single record."""
        if not self.config.field_mapping:
            return record
        
        normalized = {}
        for mapping in self.config.field_mapping:
            source_field = mapping.source_field
            target_field = mapping.target_field
            
            if source_field in record:
                value = record[source_field]
                
                # Apply transformation if specified
                if mapping.transform:
                    # Simple transformations - can be extended
                    if mapping.transform == "lower":
                        value = str(value).lower()
                    elif mapping.transform == "upper":
                        value = str(value).upper()
                    elif mapping.transform == "strip":
                        value = str(value).strip()
                    # Add more transformations as needed
                
                normalized[target_field] = value
        
        # Copy unmapped fields if they don't conflict
        target_fields = {m.target_field for m in self.config.field_mapping}
        for key, value in record.items():
            if key not in normalized and key not in target_fields:
                normalized[key] = value
        
        return normalized

    def _normalize_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply field mapping to all records."""
        return [self._normalize_record(record) for record in records]


class FetchError(Exception):
    """Base exception for fetch operations."""
    pass


class TimeoutError(FetchError):
    """Raised when fetch operation times out."""
    pass


class SecurityError(FetchError):
    """Raised when security validation fails."""
    pass


class ValidationError(FetchError):
    """Raised when data validation fails."""
    pass


# Export main classes
__all__ = [
    'BaseFetcher',
    'FetchResult', 
    'FetchError',
    'TimeoutError',
    'SecurityError',
    'ValidationError',
]