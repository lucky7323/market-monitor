"""REST API adapter."""
import asyncio
from typing import AsyncGenerator, Dict, Any
from datetime import datetime
import aiohttp

from .base import SourceAdapter, SourceError
from ..models import Record


class RestAdapter(SourceAdapter):
    """Adapter for REST API sources."""
    
    async def fetch(self) -> AsyncGenerator[Record, None]:
        """Fetch data from REST API endpoint."""
        if not self.config.url:
            raise SourceError(f"REST source {self.config.name} requires URL")
            
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        headers = self.config.headers or {}
        
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.config.url, headers=headers) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Handle different response formats
                    if isinstance(data, list):
                        # Array of records
                        for item in data:
                            yield self.normalize_record(item)
                    elif isinstance(data, dict):
                        if 'data' in data:
                            # Wrapped response with data field
                            items = data['data']
                            if isinstance(items, list):
                                for item in items:
                                    yield self.normalize_record(item)
                            else:
                                yield self.normalize_record(items)
                        else:
                            # Single record
                            yield self.normalize_record(data)
                    else:
                        raise SourceError(f"Unexpected data format from {self.config.name}")
                        
        except aiohttp.ClientError as e:
            raise SourceError(f"HTTP error from {self.config.name}: {e}")
        except Exception as e:
            raise SourceError(f"Error fetching from {self.config.name}: {e}")