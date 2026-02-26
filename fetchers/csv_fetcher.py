"""CSV fetcher for local files and remote URLs."""

import csv
import io
import time
from pathlib import Path
from typing import Any

import aiofiles

from fetchers.base import BaseFetcher, FetchResult
from security import validate_url_at_connect, validate_file_path


class CsvFetcher(BaseFetcher):
    """Fetcher for CSV sources (local file or remote URL)."""

    async def fetch(self, session) -> FetchResult:
        """Fetch data from CSV file or URL."""
        try:
            if self.config.url:
                return await self._fetch_remote_csv(session)
            elif self.config.path:
                return await self._fetch_local_csv()
            else:
                return FetchResult(
                    source_name=self.config.name,
                    error="CSV source requires either 'url' or 'path'"
                )
        except Exception as e:
            return FetchResult(
                source_name=self.config.name,
                error=f"CSV fetch error: {str(e)}"
            )

    async def _fetch_remote_csv(self, session) -> FetchResult:
        """Fetch CSV from remote URL."""
        url = self.config.url
        
        # Validate URL
        await validate_url_at_connect(url, session.allowed_hosts)
        
        start_time = time.time()
        
        try:
            async with await self._apply_timeout(
                session.get(url, headers=self.config.headers or {}),
                self.config.timeout
            ) as response:
                
                fetch_time = (time.time() - start_time) * 1000
                
                if response.status >= 400:
                    error_text = await response.text()
                    return FetchResult(
                        source_name=self.config.name,
                        fetch_time_ms=fetch_time,
                        error=f"HTTP {response.status}: {error_text[:200]}",
                        is_partial=response.status in (408, 429, 502, 503, 504)
                    )
                
                # Read CSV content
                content = await response.text()
                records = await self._parse_csv_content(content)
                
                # Apply incremental filtering
                if self.config.incremental:
                    records = self._filter_incremental_records(records)
                
                # Apply field mapping
                normalized_records = self._normalize_records(records)
                
                return FetchResult(
                    source_name=self.config.name,
                    records=normalized_records,
                    fetch_time_ms=fetch_time
                )
                
        except asyncio.TimeoutError:
            return FetchResult(
                source_name=self.config.name,
                fetch_time_ms=self.config.timeout * 1000,
                error=f"Request timeout after {self.config.timeout}s",
                is_partial=True
            )

    async def _fetch_local_csv(self) -> FetchResult:
        """Fetch CSV from local file."""
        file_path = self.config.path
        
        # Validate file path (runtime check with allowed dirs will be done by pipeline)
        validate_file_path(file_path)
        
        start_time = time.time()
        
        try:
            path_obj = Path(file_path)
            
            if not path_obj.exists():
                return FetchResult(
                    source_name=self.config.name,
                    error=f"File not found: {file_path}"
                )
            
            # Read file content
            async with aiofiles.open(path_obj, mode='r', encoding='utf-8') as f:
                content = await f.read()
            
            fetch_time = (time.time() - start_time) * 1000
            
            # Parse CSV
            records = await self._parse_csv_content(content)
            
            # Apply incremental filtering  
            if self.config.incremental:
                records = self._filter_incremental_records(records)
            
            # Apply field mapping
            normalized_records = self._normalize_records(records)
            
            return FetchResult(
                source_name=self.config.name,
                records=normalized_records,
                fetch_time_ms=fetch_time
            )
            
        except Exception as e:
            fetch_time = (time.time() - start_time) * 1000
            return FetchResult(
                source_name=self.config.name,
                fetch_time_ms=fetch_time,
                error=f"File read error: {str(e)}"
            )

    async def _parse_csv_content(self, content: str) -> list[dict[str, Any]]:
        """Parse CSV content into list of records."""
        records = []
        
        # Use StringIO to read CSV from string
        csv_reader = csv.DictReader(io.StringIO(content))
        
        for row_num, row in enumerate(csv_reader, 1):
            if row_num > 10000:  # Prevent memory exhaustion
                break
            
            # Convert row to dict with proper types
            record = {}
            for key, value in row.items():
                if value is None or value == '':
                    record[key] = None
                else:
                    # Try to convert numeric values
                    try:
                        # Try integer first
                        if '.' not in value and value.isdigit():
                            record[key] = int(value)
                        else:
                            # Try float
                            record[key] = float(value)
                    except (ValueError, TypeError):
                        # Keep as string
                        record[key] = value
            
            records.append(record)
        
        return records

    def _filter_incremental_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filter records for incremental updates."""
        last_value = self.get_incremental_filter()
        if last_value is None:
            return records
        
        field = self.config.incremental_field
        filtered = []
        
        for record in records:
            if field not in record:
                continue
            
            record_value = record[field]
            
            # Compare based on value type
            try:
                if isinstance(last_value, (int, float)) and isinstance(record_value, (int, float)):
                    if record_value > last_value:
                        filtered.append(record)
                elif isinstance(last_value, str) and isinstance(record_value, str):
                    if record_value > last_value:  # String comparison
                        filtered.append(record)
                else:
                    # Type mismatch, include record to be safe
                    filtered.append(record)
            except (TypeError, ValueError):
                # Comparison failed, include record
                filtered.append(record)
        
        return filtered

    def supports_incremental(self) -> bool:
        """Check if this fetcher supports incremental updates."""
        return True


# Export the fetcher
__all__ = ['CsvFetcher']