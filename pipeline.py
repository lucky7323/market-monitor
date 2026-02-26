"""Main pipeline orchestrator with security validation and error handling."""

import asyncio
import json
import time
from pathlib import Path
from typing import Any, TYPE_CHECKING

from config import PipelineConfig, load_config
from security import SecureClientSession, validate_file_path
from state import StateStore
from normalizer import Normalizer
from merger import Merger
from stats import StatsCollector, PipelineStats
from fetchers import *

if TYPE_CHECKING:
    from config import SourceConfig
    from fetchers.base import BaseFetcher, FetchResult


class Pipeline:
    """Multi-source data aggregation pipeline with security and error handling."""
    
    def __init__(self, config: PipelineConfig):
        """Initialize pipeline with configuration."""
        self.config = config
        self.state_store = StateStore(config.state_file)
        self.normalizer = Normalizer()
        self.merger = Merger()
        self.stats_collector = StatsCollector()
        
        # Create semaphore for concurrency control
        self.semaphore = asyncio.Semaphore(config.max_concurrent)
    
    @classmethod
    def from_config_file(cls, config_path: str) -> "Pipeline":
        """Create pipeline from configuration file."""
        config = load_config(config_path)
        return cls(config)
    
    async def run(self) -> dict[str, Any]:
        """Run the complete pipeline and return results with statistics."""
        try:
            # Validate output path
            self._validate_output_path()
            
            # Create secure HTTP session
            async with SecureClientSession(
                allowed_hosts=self.config.allowed_hosts or None,
                max_redirects=3
            ) as session:
                
                # Fetch from all sources concurrently
                all_records = []
                fetch_tasks = []
                
                for source_config in self.config.sources:
                    task = self._fetch_source_with_semaphore(source_config, session)
                    fetch_tasks.append(task)
                
                # Wait for all fetches to complete (with global timeout)
                try:
                    results = await asyncio.wait_for(
                        asyncio.gather(*fetch_tasks, return_exceptions=True),
                        timeout=self.config.global_timeout
                    )
                except asyncio.TimeoutError:
                    # Handle global timeout - cancel remaining tasks
                    for task in fetch_tasks:
                        if not task.done():
                            task.cancel()
                    
                    # Wait a bit for cancellations to complete
                    await asyncio.sleep(0.1)
                    
                    # Create partial results for completed tasks
                    results = []
                    for task in fetch_tasks:
                        if task.done() and not task.cancelled():
                            try:
                                results.append(task.result())
                            except Exception as e:
                                results.append(e)
                        else:
                            results.append(Exception("Global timeout exceeded"))
                
                # Process results
                for i, result in enumerate(results):
                    source_config = self.config.sources[i]
                    
                    if isinstance(result, Exception):
                        # Task failed or was cancelled
                        from fetchers.base import FetchResult
                        fetch_result = FetchResult(
                            source_name=source_config.name,
                            error=f"Task error: {str(result)}"
                        )
                        normalized_records = []
                    else:
                        fetch_result, normalized_records = result
                    
                    # Add to statistics
                    self.stats_collector.add_source_result(
                        source_config.name, fetch_result, normalized_records
                    )
                    
                    # Add normalized records to collection
                    all_records.extend(normalized_records)
                
                # Merge and deduplicate
                merged_records = self.merger.merge(all_records)
                
                # Update merge statistics
                duplicates_removed = len(all_records) - len(merged_records)
                self.stats_collector.set_merge_results(len(merged_records), duplicates_removed)
                
                # Commit state for successful and partial sources
                successful_sources = self.stats_collector.get_successful_sources()
                partial_sources = self.stats_collector.get_partial_sources()
                
                self.state_store.commit(successful_sources, partial_sources)
                
                # Finalize statistics (IMPORTANT: This must happen before output saving)
                final_stats = self.stats_collector.finish()
                
                # Prepare output data
                output_data = {
                    "records": merged_records,
                    "stats": final_stats.to_dict()
                }
                
                # Save output to file
                await self._save_output(output_data)
                
                return output_data
                
        except Exception as e:
            # Pipeline-level error
            from fetchers.base import FetchResult
            error_stats = self.stats_collector.finish()
            
            return {
                "records": [],
                "stats": error_stats.to_dict(),
                "error": f"Pipeline error: {str(e)}"
            }
    
    async def _fetch_source_with_semaphore(self, source_config: "SourceConfig", 
                                          session: SecureClientSession) -> tuple["FetchResult", list[dict[str, Any]]]:
        """Fetch from a single source with concurrency control."""
        async with self.semaphore:
            return await self._fetch_source(source_config, session)
    
    async def _fetch_source(self, source_config: "SourceConfig", 
                           session: SecureClientSession) -> tuple["FetchResult", list[dict[str, Any]]]:
        """Fetch and normalize data from a single source."""
        try:
            # Validate file paths if needed
            if source_config.path:
                validate_file_path(source_config.path, self.config.allowed_input_dirs)
            
            # Create appropriate fetcher
            fetcher = self._create_fetcher(source_config)
            
            # Fetch data with timeout
            fetch_result = await asyncio.wait_for(
                fetcher._timed_fetch(session),
                timeout=source_config.timeout
            )
            
            # Normalize data
            normalized_records = self.normalizer.normalize(
                fetch_result.records, 
                source_config, 
                self.state_store
            )
            
            return fetch_result, normalized_records
            
        except asyncio.TimeoutError:
            from fetchers.base import FetchResult
            return FetchResult(
                source_name=source_config.name,
                error=f"Source timeout after {source_config.timeout}s",
                is_partial=True
            ), []
        
        except Exception as e:
            from fetchers.base import FetchResult
            return FetchResult(
                source_name=source_config.name,
                error=f"Source error: {str(e)}"
            ), []
    
    def _create_fetcher(self, config: "SourceConfig") -> "BaseFetcher":
        """Create appropriate fetcher for source type."""
        fetcher_map = {
            "rest": RestFetcher,
            "csv": CsvFetcher,
            "websocket": WebSocketFetcher,
            "graphql": GraphQLFetcher,
            "file": FileFetcher,
        }
        
        fetcher_class = fetcher_map.get(config.type)
        if not fetcher_class:
            raise ValueError(f"Unsupported source type: {config.type}")
        
        return fetcher_class(config, self.state_store)
    
    def _validate_output_path(self):
        """Validate output path against allowlist."""
        output_path = Path(self.config.output)
        allowed_dirs = [Path(d).resolve() for d in self.config.allowed_output_dirs]
        
        # Resolve output path
        resolved_output = output_path.resolve()
        
        # Check if output path is within allowed directories
        is_allowed = False
        for allowed_dir in allowed_dirs:
            try:
                if resolved_output.is_relative_to(allowed_dir):
                    is_allowed = True
                    break
            except ValueError:
                # is_relative_to can raise ValueError in some cases
                continue
        
        if not is_allowed:
            raise ValueError(f"Output path {output_path} not in allowed directories: {self.config.allowed_output_dirs}")
    
    async def _save_output(self, data: dict[str, Any]):
        """Save output data to file."""
        output_path = Path(self.config.output)
        
        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write data to file
        try:
            # Try to use orjson for faster serialization if available
            try:
                import orjson
                json_bytes = orjson.dumps(data, option=orjson.OPT_INDENT_2)
                output_path.write_bytes(json_bytes)
            except ImportError:
                # Fallback to standard json
                json_str = json.dumps(data, indent=2, ensure_ascii=False, default=str)
                output_path.write_text(json_str, encoding='utf-8')
        
        except Exception as e:
            raise RuntimeError(f"Failed to save output to {output_path}: {e}")
    
    def get_state_info(self) -> dict[str, Any]:
        """Get current state information."""
        return self.state_store.health_check()
    
    def clear_state(self, source_name: str = None):
        """Clear state for a source or all sources."""
        self.state_store.clear(source_name)


# Convenience function for simple usage
async def run_pipeline_from_config(config_path: str) -> dict[str, Any]:
    """Run pipeline from configuration file."""
    pipeline = Pipeline.from_config_file(config_path)
    return await pipeline.run()


# Export main classes
__all__ = [
    'Pipeline',
    'run_pipeline_from_config',
]