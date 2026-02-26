#!/usr/bin/env python3
"""Main entry point for the multi-source data aggregation pipeline."""

import asyncio
import argparse
import logging
import json
import sys
from pathlib import Path

from pipeline.core import Pipeline, ConfigLoader
from pipeline.models import SourceConfig, SourceType


def setup_logging(level: str = "INFO"):
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('pipeline.log')
        ]
    )


async def create_demo_config() -> list[SourceConfig]:
    """Create demo configuration with mock sources."""
    return [
        # Mock REST API
        SourceConfig(
            name="jsonplaceholder_posts",
            type=SourceType.REST,
            url="https://jsonplaceholder.typicode.com/posts",
            timeout=10.0,
            field_mapping={"value": "title", "unit": "post"}
        ),
        
        # Mock CSV (we'll create a sample)
        SourceConfig(
            name="sample_csv",
            type=SourceType.CSV,
            path="./test_data/sample.csv",
            csv_delimiter=","
        ),
        
        # Mock GraphQL
        SourceConfig(
            name="countries_graphql", 
            type=SourceType.GRAPHQL,
            url="https://countries.trevorblades.com/",
            graphql_query="{ countries { name code } }"
        ),
        
        # Mock file
        SourceConfig(
            name="sample_json",
            type=SourceType.FILE,
            path="./test_data/sample.json"
        )
    ]


async def create_test_data():
    """Create test data files for demo."""
    test_dir = Path("test_data")
    test_dir.mkdir(exist_ok=True)
    
    # Create sample CSV
    csv_content = """timestamp,value,unit,source
2026-02-26T10:00:00Z,100.5,USD,market_a
2026-02-26T10:01:00Z,101.2,USD,market_a
2026-02-26T10:02:00Z,99.8,USD,market_a
"""
    with open(test_dir / "sample.csv", "w") as f:
        f.write(csv_content)
    
    # Create sample JSON
    json_content = {
        "data": [
            {"timestamp": "2026-02-26T10:00:00Z", "value": 250.5, "unit": "EUR", "source": "market_b"},
            {"timestamp": "2026-02-26T10:01:00Z", "value": 251.2, "unit": "EUR", "source": "market_b"},
        ]
    }
    with open(test_dir / "sample.json", "w") as f:
        json.dump(json_content, f, indent=2)


async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Multi-Source Data Aggregation Pipeline")
    parser.add_argument(
        "--config", 
        type=str, 
        help="Path to configuration JSON file"
    )
    parser.add_argument(
        "--output", 
        type=str, 
        default="pipeline_output.json",
        help="Output file path (default: pipeline_output.json)"
    )
    parser.add_argument(
        "--demo", 
        action="store_true",
        help="Run with demo configuration"
    )
    parser.add_argument(
        "--log-level", 
        type=str, 
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    parser.add_argument(
        "--incremental", 
        action="store_true",
        help="Enable incremental updates (only fetch new records)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        if args.demo:
            logger.info("Running demo configuration")
            await create_test_data()
            sources = await create_demo_config()
        elif args.config:
            logger.info(f"Loading configuration from {args.config}")
            sources = ConfigLoader.load_from_file(args.config)
        else:
            logger.error("Either --config or --demo must be specified")
            sys.exit(1)
        
        # Create and run pipeline
        logger.info(f"Starting pipeline with {len(sources)} sources")
        if args.incremental:
            logger.info("Incremental updates enabled")
        pipeline = Pipeline(sources, max_concurrent=5, enable_incremental=args.incremental)
        
        result = await pipeline.process()
        
        # Save results
        await pipeline.save_result(result, args.output)
        
        # Print summary
        stats = result.stats
        logger.info(f"Pipeline completed successfully!")
        logger.info(f"Sources: {stats.successful_sources}/{stats.total_sources} successful")
        logger.info(f"Records: {stats.total_records}")
        logger.info(f"Execution time: {stats.execution_time_seconds:.2f}s")
        logger.info(f"Records per second: {stats.records_per_second:.2f}")
        
        if stats.failed_sources:
            logger.warning(f"Failed sources: {', '.join(stats.failed_sources)}")
        
        print(f"✅ Results saved to {args.output}")
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())