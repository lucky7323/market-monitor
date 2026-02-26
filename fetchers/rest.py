"""REST API fetcher with security validation."""

import json
import time
from typing import Any, Optional
from urllib.parse import urlencode

from fetchers.base import BaseFetcher, FetchResult, FetchError, TimeoutError
from security import validate_url_at_connect


class RestFetcher(BaseFetcher):
    """Fetcher for REST API endpoints."""

    async def fetch(self, session) -> FetchResult:
        """Fetch data from REST API endpoint."""
        if not self.config.url:
            return FetchResult(
                source_name=self.config.name,
                error="REST source requires URL"
            )

        try:
            # Build URL with incremental filter if needed
            url = await self._build_url()
            
            # Validate URL at connect time
            await validate_url_at_connect(url, session.allowed_hosts)
            
            # Prepare headers
            headers = self.config.headers.copy() if self.config.headers else {}
            if 'User-Agent' not in headers:
                headers['User-Agent'] = 'market-monitor/1.0'
            
            # Make request with timeout
            start_time = time.time()
            
            async with await self._apply_timeout(
                session.get(url, headers=headers),
                self.config.timeout
            ) as response:
                
                fetch_time = (time.time() - start_time) * 1000
                
                # Check response status
                if response.status >= 400:
                    error_text = await response.text()
                    return FetchResult(
                        source_name=self.config.name,
                        fetch_time_ms=fetch_time,
                        error=f"HTTP {response.status}: {error_text[:200]}",
                        is_partial=response.status in (408, 429, 502, 503, 504)  # Potentially retryable
                    )
                
                # Parse response
                content_type = response.headers.get('content-type', '').lower()
                if 'application/json' in content_type:
                    data = await response.json()
                else:
                    # Try to parse as JSON anyway
                    text = await response.text()
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError:
                        return FetchResult(
                            source_name=self.config.name,
                            fetch_time_ms=fetch_time,
                            error=f"Invalid JSON response: {text[:100]}"
                        )
                
                # Extract records from response
                records = self._extract_records(data)
                
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
        except Exception as e:
            return FetchResult(
                source_name=self.config.name,
                error=f"REST fetch error: {str(e)}"
            )

    async def _build_url(self) -> str:
        """Build URL with incremental filtering parameters."""
        base_url = self.config.url
        
        # Add incremental filter if configured
        last_value = self.get_incremental_filter()
        if last_value is not None and self.config.incremental:
            # Common patterns for incremental filters
            params = {}
            
            # Try different parameter names based on field
            field = self.config.incremental_field
            if field in ('timestamp', 'created_at', 'updated_at'):
                params['since'] = last_value
            elif field in ('id', 'sequence'):
                params['after'] = last_value
            else:
                params[f'{field}_gt'] = last_value
            
            # Add parameters to URL
            separator = '&' if '?' in base_url else '?'
            query_string = urlencode(params)
            return f"{base_url}{separator}{query_string}"
        
        return base_url

    def _extract_records(self, data: Any) -> list[dict[str, Any]]:
        """Extract records from API response.
        
        Handles common response formats:
        - Direct array: [{"id": 1}, {"id": 2}]
        - Wrapped array: {"data": [{"id": 1}], "meta": {...}}
        - Single object: {"id": 1, "name": "test"}
        """
        if isinstance(data, list):
            # Direct array of records
            return [record for record in data if isinstance(record, dict)]
        
        elif isinstance(data, dict):
            # Look for common array field names
            for key in ['data', 'results', 'items', 'records']:
                if key in data and isinstance(data[key], list):
                    return [record for record in data[key] if isinstance(record, dict)]
            
            # Single record wrapped in object
            return [data]
        
        else:
            # Unexpected format
            return []

    def supports_incremental(self) -> bool:
        """Check if this fetcher supports incremental updates."""
        return True


# Export the fetcher
__all__ = ['RestFetcher']