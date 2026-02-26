"""WebSocket adapter."""
import asyncio
import json
from typing import AsyncGenerator
from datetime import datetime
import websockets

from .base import SourceAdapter, SourceError
from ..models import Record


class WebSocketAdapter(SourceAdapter):
    """Adapter for WebSocket sources."""
    
    async def fetch(self) -> AsyncGenerator[Record, None]:
        """Fetch data from WebSocket connection."""
        if not self.config.url:
            raise SourceError(f"WebSocket source {self.config.name} requires URL")
        
        try:
            # Connect to WebSocket
            async with websockets.connect(
                self.config.url,
                timeout=self.config.timeout,
                extra_headers=self.config.headers or {}
            ) as websocket:
                
                # Send subscription message if configured
                if self.config.websocket_subscription:
                    await websocket.send(self.config.websocket_subscription)
                
                # Listen for messages with timeout
                records_received = 0
                max_records = 100  # Limit for bounty requirements
                
                try:
                    while records_received < max_records:
                        # Wait for message with timeout
                        message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=self.config.timeout
                        )
                        
                        try:
                            data = json.loads(message)
                            yield self.normalize_record(data)
                            records_received += 1
                            
                        except json.JSONDecodeError:
                            self.logger.warning(f"Invalid JSON from {self.config.name}: {message}")
                            continue
                            
                except asyncio.TimeoutError:
                    self.logger.info(f"WebSocket timeout for {self.config.name} after {records_received} records")
                    return
                    
        except websockets.exceptions.ConnectionClosed as e:
            raise SourceError(f"WebSocket connection closed for {self.config.name}: {e}")
        except websockets.exceptions.InvalidURI as e:
            raise SourceError(f"Invalid WebSocket URI for {self.config.name}: {e}")
        except Exception as e:
            raise SourceError(f"WebSocket error for {self.config.name}: {e}")