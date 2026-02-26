"""Base adapter for all source types."""
import asyncio
from abc import ABC, abstractmethod
from typing import AsyncGenerator, List
from datetime import datetime
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

from ..models import SourceConfig, Record

logger = logging.getLogger(__name__)


class SourceAdapter(ABC):
    """Abstract base class for all source adapters."""
    
    def __init__(self, config: SourceConfig):
        """Initialize adapter with configuration."""
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{config.name}")
        
    @abstractmethod
    async def fetch(self) -> AsyncGenerator[Record, None]:
        """Fetch data from the source and yield normalized records.
        
        Yields:
            Record: Normalized data record
            
        Raises:
            SourceError: When source is unavailable or returns invalid data
        """
        pass
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    async def fetch_with_retry(self) -> List[Record]:
        """Fetch with automatic retry logic."""
        records = []
        try:
            async for record in self.fetch():
                records.append(record)
            return records
        except Exception as e:
            self.logger.error(f"Failed to fetch from {self.config.name}: {e}")
            raise
    
    def normalize_record(self, raw_data: dict, timestamp: datetime = None) -> Record:
        """Normalize raw data to standard record format.
        
        Args:
            raw_data: Raw data from source
            timestamp: Optional timestamp, uses current time if None
            
        Returns:
            Record: Normalized record
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
            
        # Apply field mapping if configured
        if self.config.field_mapping:
            mapped_data = {}
            for target_field, source_field in self.config.field_mapping.items():
                if source_field in raw_data:
                    mapped_data[target_field] = raw_data[source_field]
            value = mapped_data
        else:
            value = raw_data
            
        return Record(
            source=self.config.name,
            timestamp=timestamp,
            value=value,
            unit=raw_data.get('unit')
        )


class SourceError(Exception):
    """Exception raised when source adapter encounters an error."""
    pass