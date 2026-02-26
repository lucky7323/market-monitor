"""Adapter factory for creating source adapters."""
from typing import Dict, Type

from .base import SourceAdapter
from .rest import RestAdapter
from .csv_adapter import CsvAdapter
from .websocket import WebSocketAdapter
from .graphql import GraphQLAdapter
from .file import FileAdapter
from ..models import SourceConfig, SourceType


class AdapterFactory:
    """Factory for creating source adapters."""
    
    _adapters: Dict[SourceType, Type[SourceAdapter]] = {
        SourceType.REST: RestAdapter,
        SourceType.CSV: CsvAdapter,
        SourceType.WEBSOCKET: WebSocketAdapter,
        SourceType.GRAPHQL: GraphQLAdapter,
        SourceType.FILE: FileAdapter,
    }
    
    @classmethod
    def create_adapter(cls, config: SourceConfig) -> SourceAdapter:
        """Create appropriate adapter for source type.
        
        Args:
            config: Source configuration
            
        Returns:
            SourceAdapter: Adapter instance for the source type
            
        Raises:
            ValueError: If source type is not supported
        """
        adapter_class = cls._adapters.get(config.type)
        if not adapter_class:
            raise ValueError(f"Unsupported source type: {config.type}")
        
        return adapter_class(config)
    
    @classmethod
    def register_adapter(cls, source_type: SourceType, adapter_class: Type[SourceAdapter]):
        """Register a new adapter type.
        
        Args:
            source_type: Source type enum value
            adapter_class: Adapter class to register
        """
        cls._adapters[source_type] = adapter_class
    
    @classmethod
    def get_supported_types(cls) -> list[SourceType]:
        """Get list of supported source types."""
        return list(cls._adapters.keys())