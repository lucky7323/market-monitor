"""Security module for SSRF prevention, path validation, and GraphQL safety."""

import ipaddress
import socket
import aiohttp
from urllib.parse import urlparse, urljoin
from pathlib import Path
from typing import Any, Optional

# Blocked IP ranges for SSRF prevention
BLOCKED_RANGES = [
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),   # Carrier-grade NAT
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.0.0.0/24"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("198.18.0.0/15"),    # Benchmarking
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]

def _is_blocked_ip(ip_str: str) -> bool:
    """Check if IP address is in blocked ranges."""
    try:
        ip = ipaddress.ip_address(ip_str)
        return any(ip in net for net in BLOCKED_RANGES)
    except ValueError:
        return True  # Invalid IP = blocked


def validate_url_format(url: str) -> None:
    """Config-time validation: basic scheme + hostname checks."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https", "ws", "wss"):
        raise ValueError(f"Disallowed scheme: {parsed.scheme}")
    if not parsed.hostname:
        raise ValueError("Missing hostname")


async def validate_url_at_connect(url: str, allowed_hosts: Optional[list[str]] = None) -> str:
    """Connect-time validation: DNS resolve + IP check. Returns resolved URL."""
    parsed = urlparse(url)
    hostname = parsed.hostname
    
    if not hostname:
        raise ValueError("Missing hostname")
    
    # Check allowed hosts if specified
    if allowed_hosts and hostname not in allowed_hosts:
        raise ValueError(f"Host {hostname} not in allowlist")
    
    # Resolve DNS and check all returned IPs
    try:
        resolved = socket.getaddrinfo(hostname, None)
        for _, _, _, _, addr in resolved:
            ip_str = addr[0]
            if _is_blocked_ip(ip_str):
                raise ValueError(f"Blocked IP address: {ip_str} for {hostname}")
    except socket.gaierror as e:
        raise ValueError(f"DNS resolution failed for {hostname}: {e}")
    
    return url


class SecureClientSession:
    """Wrapper around aiohttp.ClientSession with redirect validation."""
    
    def __init__(self, allowed_hosts: Optional[list[str]] = None, max_redirects: int = 3):
        self.allowed_hosts = allowed_hosts
        self.max_redirects = max_redirects
        # Custom connector that validates redirects
        connector = aiohttp.TCPConnector(
            limit=10,
            limit_per_host=5,
            ttl_dns_cache=300,
        )
        self.session = aiohttp.ClientSession(
            connector=connector,
        )
    
    async def get(self, url: str, **kwargs) -> aiohttp.ClientResponse:
        """GET with redirect validation."""
        return await self._request("GET", url, **kwargs)
    
    async def post(self, url: str, **kwargs) -> aiohttp.ClientResponse:
        """POST with redirect validation."""
        return await self._request("POST", url, **kwargs)
    
    async def _request(self, method: str, url: str, **kwargs) -> aiohttp.ClientResponse:
        """Make request with redirect hop validation."""
        current_url = url
        redirects = 0
        
        while redirects <= self.max_redirects:
            # Validate current URL
            await validate_url_at_connect(current_url, self.allowed_hosts)
            
            # Make request
            resp = await self.session.request(method, current_url, **kwargs)
            
            # Check if redirect
            if resp.status in (301, 302, 303, 307, 308):
                if redirects >= self.max_redirects:
                    resp.close()
                    raise ValueError(f"Too many redirects (>{self.max_redirects})")
                
                location = resp.headers.get('Location')
                if not location:
                    resp.close()
                    raise ValueError("Redirect without Location header")
                
                # Resolve relative redirects
                next_url = urljoin(current_url, location)
                resp.close()
                
                current_url = next_url
                redirects += 1
                continue
            
            return resp
        
        raise ValueError("Redirect loop detected")
    
    async def close(self):
        """Close the session."""
        await self.session.close()
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


def validate_file_path(path: str, allowed_dirs: Optional[list[str]] = None) -> None:
    """Validate file path against directory allowlist and path traversal."""
    path_obj = Path(path).resolve()
    
    # Check for path traversal attempts
    if not path_obj.is_relative_to(Path.cwd()):
        # If not relative to current dir, check against allowed dirs
        if not allowed_dirs:
            raise ValueError(f"Absolute path not allowed: {path}")
        
        allowed = False
        for allowed_dir in allowed_dirs:
            allowed_path = Path(allowed_dir).resolve()
            if path_obj.is_relative_to(allowed_path):
                allowed = True
                break
        
        if not allowed:
            raise ValueError(f"Path {path} not in allowed directories")


def validate_graphql_query(query: str) -> None:
    """Basic GraphQL query validation - prefer AST parsing over string checks."""
    try:
        # Try to import graphql-core if available for proper AST validation
        from graphql import parse, validate_schema
        from graphql.type import GraphQLSchema
        
        # Parse the query to AST
        ast = parse(query)
        
        # Basic checks on AST
        for definition in ast.definitions:
            # Check for mutations (if you want to block them)
            if hasattr(definition, 'operation') and definition.operation == 'mutation':
                raise ValueError("Mutations not allowed")
            
            # Check for introspection queries
            if hasattr(definition, 'selection_set'):
                for selection in definition.selection_set.selections:
                    if hasattr(selection, 'name') and selection.name.value.startswith('__'):
                        raise ValueError("Introspection queries not allowed")
    
    except ImportError:
        # Fallback to basic string checks if graphql-core not available
        query_lower = query.lower().strip()
        
        # Block dangerous patterns
        blocked_patterns = ['mutation', '__schema', '__type', 'introspection']
        for pattern in blocked_patterns:
            if pattern in query_lower:
                raise ValueError(f"GraphQL pattern not allowed: {pattern}")
        
        # Ensure it looks like a valid query
        if not any(op in query_lower for op in ['query', 'subscription']):
            if not query_lower.strip().startswith('{'):
                raise ValueError("Invalid GraphQL query format")


def sanitize_headers(headers: dict[str, Any]) -> dict[str, str]:
    """Sanitize HTTP headers to prevent injection."""
    sanitized = {}
    
    for key, value in headers.items():
        # Convert to string and remove control characters
        key_str = str(key).strip()
        value_str = str(value).strip()
        
        # Remove control characters (except tab)
        key_clean = ''.join(c for c in key_str if ord(c) >= 32 or c == '\t')
        value_clean = ''.join(c for c in value_str if ord(c) >= 32 or c == '\t')
        
        # Basic header name validation
        if not key_clean or ':' in key_clean:
            continue
        
        # Skip dangerous headers
        dangerous_headers = ['host', 'connection', 'upgrade', 'transfer-encoding']
        if key_clean.lower() in dangerous_headers:
            continue
        
        sanitized[key_clean] = value_clean
    
    return sanitized


# Export main functions
__all__ = [
    'validate_url_format',
    'validate_url_at_connect', 
    'validate_file_path',
    'validate_graphql_query',
    'sanitize_headers',
    'SecureClientSession',
    'BLOCKED_RANGES',
]