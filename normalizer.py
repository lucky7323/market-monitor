"""Data normalization with watermark staging support."""

from typing import Any, TYPE_CHECKING
from datetime import datetime
import re

if TYPE_CHECKING:
    from config import SourceConfig, FieldMapping
    from state import StateStore


class Normalizer:
    """Normalizes raw data records to common schema with watermark tracking."""
    
    def normalize(self, records: list[dict[str, Any]], source_config: "SourceConfig", 
                 state_store: "StateStore") -> list[dict[str, Any]]:
        """Normalize records and stage watermark.
        
        Args:
            records: Raw records from source
            source_config: Source configuration
            state_store: State store for watermark staging
            
        Returns:
            List of normalized records
        """
        if not records:
            return []
        
        normalized = []
        
        # Normalize each record
        for record in records:
            try:
                normalized_record = self._normalize_one(record, source_config)
                if normalized_record:  # Only add if normalization succeeded
                    normalized.append(normalized_record)
            except (KeyError, ValueError, TypeError) as e:
                # Skip invalid records, but continue processing
                # The difference will be tracked in statistics
                continue
        
        # Stage watermark after normalization if incremental is enabled
        if source_config.incremental and normalized:
            self._stage_watermark(normalized, source_config, state_store)
        
        return normalized
    
    def _normalize_one(self, record: dict[str, Any], config: "SourceConfig") -> dict[str, Any]:
        """Normalize a single record."""
        if config.field_mapping:
            return self._apply_field_mapping(record, config.field_mapping, config)
        else:
            return self._apply_default_mapping(record, config)
    
    def _apply_field_mapping(self, record: dict[str, Any], 
                           mappings: list["FieldMapping"], config: "SourceConfig") -> dict[str, Any]:
        """Apply custom field mapping to record."""
        result = {"source": config.name}  # Always include source
        
        for mapping in mappings:
            source_field = mapping.source_field
            target_field = mapping.target_field
            transform = mapping.transform
            
            if source_field not in record:
                continue
            
            value = record[source_field]
            
            # Apply transformation if specified
            if transform and value is not None:
                value = self._apply_transform(value, transform)
            
            result[target_field] = value
        
        # Ensure required fields exist with defaults if not mapped
        self._ensure_required_fields(result, record)
        
        return result
    
    def _apply_default_mapping(self, record: dict[str, Any], config: "SourceConfig") -> dict[str, Any]:
        """Apply default field mapping when no custom mapping is provided."""
        # Try to intelligently map common field names
        normalized = {
            "source": config.name,
            "timestamp": self._extract_timestamp(record),
            "value": self._extract_value(record),
            "unit": self._extract_unit(record)
        }
        
        # Copy other fields as-is (excluding ones we've already mapped)
        mapped_fields = {"timestamp", "value", "unit"}
        for key, val in record.items():
            if key not in mapped_fields and not key.startswith('_'):
                normalized[key] = val
        
        return normalized
    
    def _extract_timestamp(self, record: dict[str, Any]) -> Any:
        """Extract timestamp from common field names."""
        timestamp_fields = [
            'timestamp', 'time', 'date', 'created_at', 'updated_at', 
            'datetime', 'ts', 'event_time'
        ]
        
        for field in timestamp_fields:
            if field in record and record[field] is not None:
                return self._normalize_timestamp(record[field])
        
        # If no timestamp found, use current time
        return datetime.utcnow().isoformat()
    
    def _extract_value(self, record: dict[str, Any]) -> float:
        """Extract numeric value from common field names."""
        value_fields = [
            'value', 'price', 'amount', 'count', 'quantity', 
            'total', 'sum', 'avg', 'mean'
        ]
        
        for field in value_fields:
            if field in record:
                try:
                    return float(record[field])
                except (ValueError, TypeError):
                    continue
        
        # If no numeric value found, default to 0
        return 0.0
    
    def _extract_unit(self, record: dict[str, Any]) -> str:
        """Extract unit from common field names."""
        unit_fields = ['unit', 'currency', 'symbol', 'denomination']
        
        for field in unit_fields:
            if field in record and record[field] is not None:
                return str(record[field])
        
        return "unknown"
    
    def _normalize_timestamp(self, timestamp: Any) -> str:
        """Normalize timestamp to ISO format string."""
        if isinstance(timestamp, str):
            # Already a string, try to validate/clean it
            try:
                # Try parsing as ISO format
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                return dt.isoformat()
            except ValueError:
                # Try other common formats
                return self._parse_timestamp_string(timestamp)
        
        elif isinstance(timestamp, (int, float)):
            # Unix timestamp
            try:
                dt = datetime.fromtimestamp(timestamp)
                return dt.isoformat()
            except (ValueError, OSError):
                return datetime.utcnow().isoformat()
        
        elif hasattr(timestamp, 'isoformat'):
            # datetime object
            return timestamp.isoformat()
        
        else:
            # Unknown format, use string representation
            return str(timestamp)
    
    def _parse_timestamp_string(self, timestamp_str: str) -> str:
        """Parse various timestamp string formats."""
        # Common patterns to try
        patterns = [
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO basic
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # MySQL datetime
            r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}',  # US format
            r'\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}',  # European format
        ]
        
        for pattern in patterns:
            match = re.search(pattern, timestamp_str)
            if match:
                try:
                    # Try to parse the matched portion
                    matched = match.group(0)
                    if 'T' in matched:
                        dt = datetime.fromisoformat(matched)
                    else:
                        dt = datetime.strptime(matched, '%Y-%m-%d %H:%M:%S')
                    return dt.isoformat()
                except ValueError:
                    continue
        
        # If all parsing fails, return as-is
        return timestamp_str
    
    def _apply_transform(self, value: Any, transform: str) -> Any:
        """Apply transformation to a value."""
        if value is None:
            return None
        
        transform = transform.lower().strip()
        
        if transform == "float":
            try:
                return float(value)
            except (ValueError, TypeError):
                return 0.0
        
        elif transform == "int":
            try:
                return int(float(value))
            except (ValueError, TypeError):
                return 0
        
        elif transform == "string" or transform == "str":
            return str(value)
        
        elif transform == "lower":
            return str(value).lower()
        
        elif transform == "upper":
            return str(value).upper()
        
        elif transform == "strip":
            return str(value).strip()
        
        elif transform == "isoformat":
            # Assume it's already in ISO format or convert if needed
            return self._normalize_timestamp(value)
        
        else:
            # Unknown transform, return as-is
            return value
    
    def _ensure_required_fields(self, result: dict[str, Any], original: dict[str, Any]):
        """Ensure required fields exist with reasonable defaults."""
        # Ensure timestamp exists
        if "timestamp" not in result:
            result["timestamp"] = self._extract_timestamp(original)
        
        # Ensure value exists
        if "value" not in result:
            result["value"] = self._extract_value(original)
        
        # Ensure unit exists
        if "unit" not in result:
            result["unit"] = self._extract_unit(original)
    
    def _stage_watermark(self, normalized_records: list[dict[str, Any]], 
                        config: "SourceConfig", state_store: "StateStore"):
        """Stage watermark based on normalized records."""
        watermark_field = config.incremental_field
        
        # Collect all watermark values from normalized records
        watermark_values = []
        for record in normalized_records:
            if watermark_field in record and record[watermark_field] is not None:
                watermark_values.append(record[watermark_field])
        
        if watermark_values:
            # Find the maximum watermark value
            try:
                # Handle different types appropriately
                if all(isinstance(v, (int, float)) for v in watermark_values):
                    max_watermark = max(watermark_values)
                elif all(isinstance(v, str) for v in watermark_values):
                    max_watermark = max(watermark_values)
                else:
                    # Mixed types, convert to strings and find max
                    max_watermark = max(str(v) for v in watermark_values)
                
                # Stage the watermark
                state_store.stage_watermark(config.name, max_watermark)
                
            except (TypeError, ValueError):
                # If comparison fails, stage the last value
                state_store.stage_watermark(config.name, watermark_values[-1])


# Export main class
__all__ = ['Normalizer']