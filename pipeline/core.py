"""Core pipeline implementation."""
import asyncio
import json
from typing import List, Dict, Any
from datetime import datetime
import time
import logging

from .models import SourceConfig, Record, PipelineResult, PipelineStats
from .adapters.factory import AdapterFactory
from .adapters.base import SourceError
from .incremental import IncrementalTracker

logger = logging.getLogger(__name__)


class Pipeline:
    """Multi-source data aggregation pipeline."""
    
    def __init__(self, sources: List[SourceConfig], max_concurrent: int = 5, enable_incremental: bool = False):
        """Initialize pipeline with source configurations.
        
        Args:
            sources: List of source configurations
            max_concurrent: Maximum concurrent source fetches
            enable_incremental: Enable incremental updates
        """
        self.sources = sources
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.incremental_tracker = IncrementalTracker() if enable_incremental else None
    
    async def process(self) -> PipelineResult:
        """Process all sources and return aggregated result.
        
        Returns:
            PipelineResult: Aggregated and sorted records with statistics
        """
        start_time = time.time()
        all_records = []
        failed_sources = []
        successful_sources = 0
        
        # Create semaphore for concurrent limiting
        tasks = []
        for source_config in self.sources:
            task = asyncio.create_task(
                self._fetch_from_source(source_config, failed_sources)
            )
            tasks.append(task)
        
        # Wait for all sources to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed_sources.append(self.sources[i].name)
                logger.error(f"Source {self.sources[i].name} failed: {result}")
            elif result:
                all_records.extend(result)
                successful_sources += 1
        
        # Sort records by timestamp
        all_records.sort(key=lambda r: r.timestamp)
        
        # Calculate statistics
        end_time = time.time()
        execution_time = end_time - start_time
        
        stats = PipelineStats(
            total_sources=len(self.sources),
            successful_sources=successful_sources,
            failed_sources=failed_sources,
            total_records=len(all_records),
            execution_time_seconds=execution_time,
            records_per_second=len(all_records) / execution_time if execution_time > 0 else 0
        )
        
        return PipelineResult(
            records=all_records,
            stats=stats
        )
    
    async def _fetch_from_source(
        self, 
        source_config: SourceConfig, 
        failed_sources: List[str]
    ) -> List[Record]:
        """Fetch data from a single source with concurrency control.
        
        Args:
            source_config: Source configuration
            failed_sources: List to track failed sources
            
        Returns:
            List[Record]: Records from the source
        """
        async with self.semaphore:
            try:
                # Check if we should skip this source for incremental updates
                if self.incremental_tracker and self.incremental_tracker.should_skip_source(source_config):
                    logger.info(f"Skipping {source_config.name} (no updates since last run)")
                    return []
                
                adapter = AdapterFactory.create_adapter(source_config)
                records = await adapter.fetch_with_retry()
                
                # Update incremental tracking
                if self.incremental_tracker and records:
                    self.incremental_tracker.update_last_update(
                        source_config.name,
                        datetime.utcnow(),
                        len(records)
                    )
                
                logger.info(f"Successfully fetched {len(records)} records from {source_config.name}")
                return records
                
            except SourceError as e:
                logger.error(f"Source error for {source_config.name}: {e}")
                return []
            except Exception as e:
                logger.error(f"Unexpected error for {source_config.name}: {e}")
                return []
    
    async def save_result(self, result: PipelineResult, output_path: str = "output.json"):
        """Save pipeline result to JSON file.
        
        Args:
            result: Pipeline result to save
            output_path: Output file path
        """
        output_data = {
            "records": [record.dict() for record in result.records],
            "stats": result.stats.dict(),
            "generated_at": datetime.utcnow().isoformat()
        }
        
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
        
        logger.info(f"Results saved to {output_path}")


class ConfigLoader:
    """Utility class for loading pipeline configuration."""
    
    @staticmethod
    def load_from_dict(config_dict: Dict[str, Any]) -> List[SourceConfig]:
        """Load source configurations from dictionary.
        
        Args:
            config_dict: Configuration dictionary
            
        Returns:
            List[SourceConfig]: List of source configurations
        """
        sources = []
        for source_data in config_dict.get('sources', []):
            sources.append(SourceConfig(**source_data))
        return sources
    
    @staticmethod
    def load_from_file(config_path: str) -> List[SourceConfig]:
        """Load source configurations from JSON file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            List[SourceConfig]: List of source configurations
        """
        with open(config_path, 'r') as f:
            config_dict = json.load(f)
        return ConfigLoader.load_from_dict(config_dict)