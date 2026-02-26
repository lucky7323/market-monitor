"""CSV file adapter."""
import asyncio
from typing import AsyncGenerator
import csv
from datetime import datetime
import aiofiles
import aiohttp

from .base import SourceAdapter, SourceError
from ..models import Record


class CsvAdapter(SourceAdapter):
    """Adapter for CSV sources (local file or remote URL)."""
    
    async def fetch(self) -> AsyncGenerator[Record, None]:
        """Fetch data from CSV file or URL."""
        if self.config.url:
            # Remote CSV file
            async for record in self._fetch_remote_csv():
                yield record
        elif self.config.path:
            # Local CSV file
            async for record in self._fetch_local_csv():
                yield record
        else:
            raise SourceError(f"CSV source {self.config.name} requires URL or path")
    
    async def _fetch_remote_csv(self) -> AsyncGenerator[Record, None]:
        """Fetch CSV from remote URL."""
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        headers = self.config.headers or {}
        
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.config.url, headers=headers) as response:
                    response.raise_for_status()
                    text = await response.text()
                    
                    # Parse CSV content
                    reader = csv.DictReader(
                        text.splitlines(),
                        delimiter=self.config.csv_delimiter
                    )
                    
                    for row in reader:
                        yield self.normalize_record(dict(row))
                        
        except aiohttp.ClientError as e:
            raise SourceError(f"HTTP error from {self.config.name}: {e}")
        except Exception as e:
            raise SourceError(f"Error parsing CSV from {self.config.name}: {e}")
    
    async def _fetch_local_csv(self) -> AsyncGenerator[Record, None]:
        """Fetch CSV from local file."""
        try:
            async with aiofiles.open(self.config.path, 'r') as file:
                content = await file.read()
                
                # Parse CSV content
                reader = csv.DictReader(
                    content.splitlines(),
                    delimiter=self.config.csv_delimiter
                )
                
                for row in reader:
                    yield self.normalize_record(dict(row))
                    
        except FileNotFoundError:
            raise SourceError(f"CSV file not found: {self.config.path}")
        except Exception as e:
            raise SourceError(f"Error reading CSV file {self.config.name}: {e}")