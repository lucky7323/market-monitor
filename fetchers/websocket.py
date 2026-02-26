"""WebSocket fetcher for real-time data streams."""

import json
import time
import asyncio
from typing import Any, Optional

import websockets
from websockets.exceptions import WebSocketException, ConnectionClosedError

from fetchers.base import BaseFetcher, FetchResult
from security import validate_url_at_connect


class WebSocketFetcher(BaseFetcher):
    """Fetcher for WebSocket data streams."""

    async def fetch(self, session) -> FetchResult:
        """Fetch data from WebSocket connection."""
        if not self.config.url:
            return FetchResult(
                source_name=self.config.name,
                error="WebSocket source requires URL"
            )

        try:
            # Validate WebSocket URL
            url = self.config.url
            await validate_url_at_connect(url, session.allowed_hosts)
            
            # Collect messages for configured duration
            records = []
            start_time = time.time()
            
            # WebSocket connection parameters
            headers = self.config.headers.copy() if self.config.headers else {}
            duration = self.config.ws_duration
            
            try:
                # Connect to WebSocket
                async with websockets.connect(
                    url,
                    extra_headers=headers,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10
                ) as websocket:
                    
                    # Collect messages for specified duration
                    end_time = start_time + duration
                    
                    while time.time() < end_time:
                        try:
                            # Calculate remaining timeout
                            remaining = end_time - time.time()
                            if remaining <= 0:
                                break
                            
                            # Receive message with timeout
                            message = await asyncio.wait_for(
                                websocket.recv(),
                                timeout=min(remaining, 1.0)  # Check time every second
                            )
                            
                            # Parse message
                            record = self._parse_message(message)
                            if record:
                                records.append(record)
                            
                            # Limit number of records to prevent memory issues
                            if len(records) >= 1000:
                                break
                                
                        except asyncio.TimeoutError:
                            # No message received, continue collecting
                            continue
                        except ConnectionClosedError:
                            # Connection closed by server
                            break
            
            except WebSocketException as e:
                fetch_time = (time.time() - start_time) * 1000
                return FetchResult(
                    source_name=self.config.name,
                    fetch_time_ms=fetch_time,
                    error=f"WebSocket error: {str(e)}",
                    is_partial=True
                )
            
            except asyncio.TimeoutError:
                fetch_time = (time.time() - start_time) * 1000
                return FetchResult(
                    source_name=self.config.name,
                    fetch_time_ms=fetch_time,
                    error=f"WebSocket connection timeout after {self.config.timeout}s",
                    is_partial=True
                )
            
            fetch_time = (time.time() - start_time) * 1000
            
            # Apply incremental filtering
            if self.config.incremental:
                records = self._filter_incremental_records(records)
            
            # Apply field mapping
            normalized_records = self._normalize_records(records)
            
            return FetchResult(
                source_name=self.config.name,
                records=normalized_records,
                fetch_time_ms=fetch_time,
                is_partial=len(records) >= 1000  # Partial if we hit the limit
            )
            
        except Exception as e:
            return FetchResult(
                source_name=self.config.name,
                error=f"WebSocket fetch error: {str(e)}"
            )

    def _parse_message(self, message: str) -> Optional[dict[str, Any]]:
        """Parse WebSocket message into a record."""
        try:
            # Try to parse as JSON
            data = json.loads(message)
            
            if isinstance(data, dict):
                return data
            elif isinstance(data, list) and len(data) > 0:
                # Return first item if it's a list
                return data[0] if isinstance(data[0], dict) else None
            else:
                # Wrap primitive values
                return {"value": data, "message": message}
                
        except json.JSONDecodeError:
            # Not JSON, treat as text message
            return {"message": message, "type": "text"}
        except Exception:
            # Parsing failed
            return None

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
                    # Type mismatch or comparison not possible, include record
                    filtered.append(record)
            except (TypeError, ValueError):
                # Comparison failed, include record
                filtered.append(record)
        
        return filtered

    def supports_incremental(self) -> bool:
        """Check if this fetcher supports incremental updates."""
        return True

    async def _apply_timeout(self, coro, timeout: float):
        """Apply timeout with proper WebSocket handling."""
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(f"WebSocket operation timed out after {timeout}s")


# Export the fetcher
__all__ = ['WebSocketFetcher']