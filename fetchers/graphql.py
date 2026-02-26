"""GraphQL fetcher with query validation and security checks."""

import asyncio
import json
import time
from typing import Any

from fetchers.base import BaseFetcher, FetchResult
from security import validate_url_at_connect, validate_graphql_query


class GraphQLFetcher(BaseFetcher):
    """Fetcher for GraphQL endpoints."""

    async def fetch(self, session) -> FetchResult:
        """Fetch data from GraphQL endpoint."""
        if not self.config.url:
            return FetchResult(
                source_name=self.config.name,
                error="GraphQL source requires URL"
            )
        
        if not self.config.query:
            return FetchResult(
                source_name=self.config.name,
                error="GraphQL source requires query"
            )

        try:
            # Validate URL
            url = self.config.url
            await validate_url_at_connect(url, session.allowed_hosts)
            
            # Validate GraphQL query for security
            validate_graphql_query(self.config.query)
            
            # Build GraphQL request payload
            payload = self._build_payload()
            
            # Prepare headers
            headers = self.config.headers.copy() if self.config.headers else {}
            headers.update({
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            })
            if 'User-Agent' not in headers:
                headers['User-Agent'] = 'market-monitor/1.0'
            
            start_time = time.time()
            
            # Make GraphQL request
            async with await self._apply_timeout(
                session.post(url, json=payload, headers=headers),
                self.config.timeout
            ) as response:
                
                fetch_time = (time.time() - start_time) * 1000
                
                # Check response status
                if response.status >= 400:
                    error_text = await response.text()
                    return FetchResult(
                        source_name=self.config.name,
                        fetch_time_ms=fetch_time,
                        error=f"GraphQL HTTP {response.status}: {error_text[:200]}",
                        is_partial=response.status in (408, 429, 502, 503, 504)
                    )
                
                # Parse JSON response
                try:
                    data = await response.json()
                except json.JSONDecodeError as e:
                    return FetchResult(
                        source_name=self.config.name,
                        fetch_time_ms=fetch_time,
                        error=f"Invalid JSON response: {str(e)}"
                    )
                
                # Check for GraphQL errors
                if 'errors' in data:
                    errors = data['errors']
                    error_messages = [err.get('message', str(err)) for err in errors]
                    return FetchResult(
                        source_name=self.config.name,
                        fetch_time_ms=fetch_time,
                        error=f"GraphQL errors: {'; '.join(error_messages[:3])}",
                        is_partial=len(error_messages) > 0
                    )
                
                # Extract records from GraphQL response
                records = self._extract_records(data.get('data', {}))
                
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
                error=f"GraphQL request timeout after {self.config.timeout}s",
                is_partial=True
            )
        except Exception as e:
            return FetchResult(
                source_name=self.config.name,
                error=f"GraphQL fetch error: {str(e)}"
            )

    def _build_payload(self) -> dict[str, Any]:
        """Build GraphQL request payload with incremental filtering."""
        payload = {
            'query': self.config.query
        }
        
        # Add variables if configured
        variables = self.config.variables.copy() if self.config.variables else {}
        
        # Add incremental filter to variables
        if self.config.incremental:
            last_value = self.get_incremental_filter()
            if last_value is not None:
                # Add the last processed value as a variable
                field = self.config.incremental_field
                
                # Common variable names for incremental queries
                if field in ('timestamp', 'created_at', 'updated_at'):
                    variables['since'] = last_value
                elif field in ('id', 'sequence'):
                    variables['after'] = last_value
                else:
                    variables[f'{field}_gt'] = last_value
        
        if variables:
            payload['variables'] = variables
        
        return payload

    def _extract_records(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        """Extract records from GraphQL response data.
        
        Handles common GraphQL response patterns:
        - Direct array field: { "items": [{"id": 1}, {"id": 2}] }
        - Nested object with edges: { "connection": { "edges": [{"node": {"id": 1}}] } }
        - Single object: { "item": {"id": 1} }
        """
        records = []
        
        def extract_from_value(value: Any) -> list[dict[str, Any]]:
            """Recursively extract records from any value."""
            if isinstance(value, list):
                # Array of items
                extracted = []
                for item in value:
                    if isinstance(item, dict):
                        # Check if it's a GraphQL edge with node
                        if 'node' in item and isinstance(item['node'], dict):
                            extracted.append(item['node'])
                        else:
                            extracted.append(item)
                return extracted
            elif isinstance(value, dict):
                # Single object or container
                # Check for common GraphQL connection patterns
                if 'edges' in value and isinstance(value['edges'], list):
                    # GraphQL connection with edges
                    return extract_from_value(value['edges'])
                elif 'nodes' in value and isinstance(value['nodes'], list):
                    # GraphQL connection with nodes
                    return value['nodes']
                else:
                    # Single record
                    return [value]
            else:
                return []
        
        # Try to extract from common field names
        for field_name in ['data', 'items', 'results', 'records']:
            if field_name in data:
                records.extend(extract_from_value(data[field_name]))
        
        # If no common fields found, try extracting from all top-level fields
        if not records:
            for key, value in data.items():
                if isinstance(value, (list, dict)):
                    records.extend(extract_from_value(value))
        
        # Filter to only include dict records
        return [record for record in records if isinstance(record, dict)]

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
                    if record_value > last_value:
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
__all__ = ['GraphQLFetcher']