"""GraphQL adapter."""
import asyncio
from typing import AsyncGenerator, Dict, Any
from datetime import datetime
import aiohttp

from .base import SourceAdapter, SourceError
from ..models import Record


class GraphQLAdapter(SourceAdapter):
    """Adapter for GraphQL sources."""
    
    async def fetch(self) -> AsyncGenerator[Record, None]:
        """Fetch data from GraphQL endpoint."""
        if not self.config.url:
            raise SourceError(f"GraphQL source {self.config.name} requires URL")
        
        if not self.config.graphql_query:
            raise SourceError(f"GraphQL source {self.config.name} requires query")
        
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        headers = {
            'Content-Type': 'application/json',
            **(self.config.headers or {})
        }
        
        payload = {
            'query': self.config.graphql_query
        }
        
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    self.config.url,
                    json=payload,
                    headers=headers
                ) as response:
                    response.raise_for_status()
                    result = await response.json()
                    
                    # Check for GraphQL errors
                    if 'errors' in result:
                        error_msgs = [error.get('message', 'Unknown error') for error in result['errors']]
                        raise SourceError(f"GraphQL errors from {self.config.name}: {'; '.join(error_msgs)}")
                    
                    if 'data' not in result:
                        raise SourceError(f"No data in GraphQL response from {self.config.name}")
                    
                    # Extract data based on query structure
                    data = result['data']
                    
                    # Handle different response structures
                    async for record in self._process_graphql_data(data):
                        yield record
                    
        except aiohttp.ClientError as e:
            raise SourceError(f"HTTP error from {self.config.name}: {e}")
        except Exception as e:
            raise SourceError(f"Error fetching from GraphQL {self.config.name}: {e}")
    
    async def _process_graphql_data(self, data: Dict[str, Any]) -> AsyncGenerator[Record, None]:
        """Process GraphQL data and yield records."""
        # Find the main data array/object
        for key, value in data.items():
            if isinstance(value, list):
                # Array of records
                for item in value:
                    yield self.normalize_record(item)
            elif isinstance(value, dict):
                # Single record or nested structure
                if self._looks_like_record(value):
                    yield self.normalize_record(value)
                else:
                    # Nested structure, recurse
                    async for record in self._process_graphql_data(value):
                        yield record
    
    def _looks_like_record(self, obj: Dict[str, Any]) -> bool:
        """Determine if object looks like a data record."""
        # Simple heuristic: if it has value-like fields
        value_keys = ['value', 'price', 'amount', 'quantity', 'count', 'total']
        return any(key in obj for key in value_keys)