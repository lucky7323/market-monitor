"""File adapter for JSON and other file types."""
import asyncio
import json
from typing import AsyncGenerator
from datetime import datetime
import aiofiles

from .base import SourceAdapter, SourceError
from ..models import Record


class FileAdapter(SourceAdapter):
    """Adapter for file sources (JSON, text)."""
    
    async def fetch(self) -> AsyncGenerator[Record, None]:
        """Fetch data from file."""
        if not self.config.path:
            raise SourceError(f"File source {self.config.name} requires path")
        
        try:
            async with aiofiles.open(self.config.path, 'r') as file:
                content = await file.read()
                
                # Determine file type and parse accordingly
                if self.config.path.endswith('.json'):
                    async for record in self._process_json_file(content):
                        yield record
                elif self.config.path.endswith('.jsonl'):
                    async for record in self._process_jsonl_file(content):
                        yield record
                else:
                    # Assume JSON by default
                    async for record in self._process_json_file(content):
                        yield record
                    
        except FileNotFoundError:
            raise SourceError(f"File not found: {self.config.path}")
        except json.JSONDecodeError as e:
            raise SourceError(f"Invalid JSON in file {self.config.path}: {e}")
        except Exception as e:
            raise SourceError(f"Error reading file {self.config.name}: {e}")
    
    async def _process_json_file(self, content: str) -> AsyncGenerator[Record, None]:
        """Process JSON file content."""
        try:
            data = json.loads(content)
            
            if isinstance(data, list):
                # Array of records
                for item in data:
                    yield self.normalize_record(item)
            elif isinstance(data, dict):
                if 'data' in data and isinstance(data['data'], list):
                    # Wrapped array
                    for item in data['data']:
                        yield self.normalize_record(item)
                else:
                    # Single record
                    yield self.normalize_record(data)
            else:
                raise SourceError(f"Unexpected JSON structure in {self.config.path}")
                
        except json.JSONDecodeError as e:
            raise SourceError(f"Invalid JSON in {self.config.path}: {e}")
    
    async def _process_jsonl_file(self, content: str) -> AsyncGenerator[Record, None]:
        """Process JSON Lines file content."""
        for line_num, line in enumerate(content.strip().split('\n'), 1):
            if not line.strip():
                continue
                
            try:
                data = json.loads(line)
                yield self.normalize_record(data)
            except json.JSONDecodeError as e:
                self.logger.warning(f"Invalid JSON at line {line_num} in {self.config.path}: {e}")
                continue