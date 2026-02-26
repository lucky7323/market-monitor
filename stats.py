"""Pipeline statistics tracking with comprehensive source status monitoring."""

from dataclasses import dataclass, field, asdict
from typing import Literal, Any, Optional
from datetime import datetime


@dataclass
class SourceStats:
    """Statistics for a single source."""
    name: str
    status: Literal["success", "failed", "timeout", "partial"] = "success"
    records_fetched: int = 0
    records_after_normalize: int = 0
    fetch_time_ms: float = 0.0
    error: Optional[str] = None
    
    @property
    def success(self) -> bool:
        """True if source was successful."""
        return self.status == "success"
    
    @property
    def failed(self) -> bool:
        """True if source failed completely."""
        return self.status in ("failed", "timeout")
    
    @property
    def partial(self) -> bool:
        """True if source had partial success."""
        return self.status == "partial"


@dataclass
class PipelineStats:
    """Overall pipeline execution statistics."""
    started_at: str = ""
    finished_at: str = ""
    total_time_ms: float = 0.0
    sources: list[SourceStats] = field(default_factory=list)
    total_records: int = 0
    duplicates_removed: int = 0
    
    def __post_init__(self):
        """Initialize timestamps if not provided."""
        if not self.started_at:
            self.started_at = datetime.utcnow().isoformat()
    
    def add_source_stats(self, source_stats: SourceStats):
        """Add statistics for a source."""
        self.sources.append(source_stats)
    
    def set_finished(self):
        """Mark pipeline as finished and calculate total time."""
        self.finished_at = datetime.utcnow().isoformat()
        
        if self.started_at:
            try:
                start_dt = datetime.fromisoformat(self.started_at.replace('Z', '+00:00'))
                end_dt = datetime.fromisoformat(self.finished_at.replace('Z', '+00:00'))
                self.total_time_ms = (end_dt - start_dt).total_seconds() * 1000
            except ValueError:
                # If parsing fails, keep total_time_ms as is
                pass
    
    @property
    def failed_sources(self) -> list[str]:
        """List of sources that failed completely."""
        return [s.name for s in self.sources if s.failed]
    
    @property
    def partial_sources(self) -> list[str]:
        """List of sources that had partial success."""
        return [s.name for s in self.sources if s.partial]
    
    @property
    def successful_sources(self) -> list[str]:
        """List of sources that were completely successful."""
        return [s.name for s in self.sources if s.success]
    
    @property
    def success_rate(self) -> float:
        """Success rate as percentage (0.0 to 1.0)."""
        if not self.sources:
            return 0.0
        
        successful_count = sum(1 for s in self.sources if s.success or s.partial)
        return successful_count / len(self.sources)
    
    @property
    def total_fetch_time_ms(self) -> float:
        """Total time spent fetching from all sources."""
        return sum(s.fetch_time_ms for s in self.sources)
    
    @property
    def average_fetch_time_ms(self) -> float:
        """Average fetch time per source."""
        if not self.sources:
            return 0.0
        return self.total_fetch_time_ms / len(self.sources)
    
    @property
    def total_records_fetched(self) -> int:
        """Total records fetched across all sources."""
        return sum(s.records_fetched for s in self.sources)
    
    @property
    def total_records_normalized(self) -> int:
        """Total records after normalization."""
        return sum(s.records_after_normalize for s in self.sources)
    
    @property
    def normalization_loss_rate(self) -> float:
        """Rate of records lost during normalization (0.0 to 1.0)."""
        fetched = self.total_records_fetched
        if fetched == 0:
            return 0.0
        
        normalized = self.total_records_normalized
        return (fetched - normalized) / fetched
    
    @property
    def deduplication_rate(self) -> float:
        """Rate of records removed as duplicates (0.0 to 1.0)."""
        normalized = self.total_records_normalized
        if normalized == 0:
            return 0.0
        
        return self.duplicates_removed / (self.total_records + self.duplicates_removed)
    
    @property
    def has_errors(self) -> bool:
        """True if any source had errors."""
        return any(s.error is not None for s in self.sources)
    
    @property
    def error_summary(self) -> list[str]:
        """List of error messages from failed sources."""
        errors = []
        for source in self.sources:
            if source.error:
                errors.append(f"{source.name}: {source.error}")
        return errors
    
    def get_source_stats(self, source_name: str) -> Optional[SourceStats]:
        """Get statistics for a specific source."""
        for stats in self.sources:
            if stats.name == source_name:
                return stats
        return None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return asdict(self)
    
    def get_summary(self) -> dict[str, Any]:
        """Get a summary of key statistics."""
        return {
            "total_time_ms": self.total_time_ms,
            "sources_total": len(self.sources),
            "sources_successful": len(self.successful_sources),
            "sources_partial": len(self.partial_sources),
            "sources_failed": len(self.failed_sources),
            "success_rate": self.success_rate,
            "records_fetched": self.total_records_fetched,
            "records_normalized": self.total_records_normalized,
            "records_final": self.total_records,
            "duplicates_removed": self.duplicates_removed,
            "average_fetch_time_ms": self.average_fetch_time_ms,
            "has_errors": self.has_errors
        }


class StatsCollector:
    """Utility class for collecting and managing pipeline statistics."""
    
    def __init__(self):
        self.stats = PipelineStats()
    
    def create_source_stats(self, source_name: str) -> SourceStats:
        """Create a new SourceStats instance."""
        return SourceStats(name=source_name)
    
    def add_source_result(self, source_name: str, fetch_result, normalized_records: list):
        """Add results from a source fetch and normalization."""
        from fetchers.base import FetchResult
        
        if not isinstance(fetch_result, FetchResult):
            raise ValueError("fetch_result must be a FetchResult instance")
        
        # Determine status
        if fetch_result.error:
            if fetch_result.is_partial:
                status = "partial"
            else:
                status = "timeout" if "timeout" in fetch_result.error.lower() else "failed"
        else:
            status = "success"
        
        # Create source stats
        source_stats = SourceStats(
            name=source_name,
            status=status,
            records_fetched=len(fetch_result.records),
            records_after_normalize=len(normalized_records),
            fetch_time_ms=fetch_result.fetch_time_ms,
            error=fetch_result.error
        )
        
        self.stats.add_source_stats(source_stats)
    
    def set_merge_results(self, total_records: int, duplicates_removed: int):
        """Set results from the merge/deduplication phase."""
        self.stats.total_records = total_records
        self.stats.duplicates_removed = duplicates_removed
    
    def finish(self) -> PipelineStats:
        """Mark pipeline as finished and return final stats."""
        self.stats.set_finished()
        return self.stats
    
    def get_successful_sources(self) -> set[str]:
        """Get set of source names that were completely successful."""
        return set(self.stats.successful_sources)
    
    def get_partial_sources(self) -> set[str]:
        """Get set of source names that had partial success."""
        return set(self.stats.partial_sources)
    
    def get_failed_sources(self) -> set[str]:
        """Get set of source names that failed completely."""
        return set(self.stats.failed_sources)


# Export main classes
__all__ = [
    'SourceStats',
    'PipelineStats', 
    'StatsCollector',
]