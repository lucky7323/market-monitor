"""Data merger with stable hash-based deduplication and sorting."""

import json
import hashlib
from typing import Any


class Merger:
    """Merges and deduplicates records from multiple sources."""
    
    def merge(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Merge and deduplicate records, then sort by timestamp.
        
        Args:
            records: List of normalized records from all sources
            
        Returns:
            Deduplicated and sorted list of records
        """
        if not records:
            return []
        
        # Deduplicate using stable hash
        deduplicated = self._deduplicate(records)
        
        # Sort by timestamp
        sorted_records = self._sort_by_timestamp(deduplicated)
        
        return sorted_records
    
    def _deduplicate(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Remove duplicate records using stable hash."""
        seen_hashes = set()
        deduplicated = []
        
        for record in records:
            record_hash = self._stable_hash(record)
            
            if record_hash not in seen_hashes:
                seen_hashes.add(record_hash)
                deduplicated.append(record)
        
        return deduplicated
    
    def _stable_hash(self, record: dict[str, Any]) -> str:
        """Generate a stable hash for a record.
        
        This hash should be consistent across runs for the same record content,
        even if the order of dict keys changes.
        """
        # Convert record to a canonical JSON string for hashing
        canonical_json = self._to_canonical_json(record)
        
        # Generate SHA-256 hash
        hash_obj = hashlib.sha256(canonical_json.encode('utf-8'))
        return hash_obj.hexdigest()
    
    def _to_canonical_json(self, obj: Any) -> str:
        """Convert object to canonical JSON string.
        
        This ensures that identical data structures always produce
        the same JSON representation, regardless of key order.
        """
        return json.dumps(
            obj,
            sort_keys=True,     # Sort dictionary keys
            separators=(',', ':'),  # No extra whitespace
            ensure_ascii=True,  # ASCII encoding for consistency
            default=self._json_default
        )
    
    def _json_default(self, obj: Any) -> Any:
        """Handle non-serializable objects for JSON encoding."""
        # Convert datetime objects to ISO string
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        
        # Convert other objects to string representation
        return str(obj)
    
    def _sort_by_timestamp(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Sort records by timestamp field."""
        def get_sort_key(record: dict[str, Any]) -> tuple:
            """Generate sort key for a record.
            
            Primary sort: timestamp (if present)
            Secondary sort: source name
            Tertiary sort: stable hash (for consistent ordering)
            """
            timestamp = record.get('timestamp', '')
            source = record.get('source', '')
            record_hash = self._stable_hash(record)
            
            # Convert timestamp to sortable format
            timestamp_key = self._normalize_timestamp_for_sorting(timestamp)
            
            return (timestamp_key, source, record_hash)
        
        try:
            return sorted(records, key=get_sort_key)
        except (TypeError, ValueError) as e:
            # If sorting fails, return records in original order
            # This ensures the pipeline doesn't crash due to sorting issues
            return records
    
    def _normalize_timestamp_for_sorting(self, timestamp: Any) -> str:
        """Normalize timestamp for consistent sorting."""
        if timestamp is None:
            return ''
        
        if isinstance(timestamp, str):
            return timestamp
        
        elif isinstance(timestamp, (int, float)):
            # Convert Unix timestamp to ISO string
            try:
                from datetime import datetime
                dt = datetime.fromtimestamp(timestamp)
                return dt.isoformat()
            except (ValueError, OSError):
                return str(timestamp)
        
        elif hasattr(timestamp, 'isoformat'):
            return timestamp.isoformat()
        
        else:
            return str(timestamp)
    
    def calculate_dedup_stats(self, original_count: int, deduplicated_count: int) -> dict[str, Any]:
        """Calculate deduplication statistics."""
        if original_count == 0:
            return {
                'original_count': 0,
                'deduplicated_count': 0,
                'duplicates_removed': 0,
                'deduplication_rate': 0.0
            }
        
        duplicates_removed = original_count - deduplicated_count
        deduplication_rate = duplicates_removed / original_count
        
        return {
            'original_count': original_count,
            'deduplicated_count': deduplicated_count,
            'duplicates_removed': duplicates_removed,
            'deduplication_rate': deduplication_rate
        }
    
    def merge_with_stats(self, records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Merge records and return both results and statistics."""
        original_count = len(records)
        
        # Perform merge
        merged_records = self.merge(records)
        deduplicated_count = len(merged_records)
        
        # Calculate stats
        stats = self.calculate_dedup_stats(original_count, deduplicated_count)
        
        return merged_records, stats


# Export main class
__all__ = ['Merger']