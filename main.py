#!/usr/bin/env python3
"""Main CLI entry point for the market monitor pipeline."""

import asyncio
import argparse
import logging
import sys
from pathlib import Path

from pipeline import Pipeline
from config import create_example_config
import json


def setup_logging(level: str = "INFO", log_file: str = None):
    """Setup logging configuration."""
    handlers = [logging.StreamHandler()]
    
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )


async def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Multi-source data aggregation pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s config.yaml                    # Run with config file
  %(prog)s --create-example config.yaml  # Create example config
  %(prog)s --config config.yaml --output results.json
        """
    )
    
    parser.add_argument(
        'config',
        nargs='?',
        help='Configuration file path (YAML or JSON)'
    )
    
    parser.add_argument(
        '--output', '-o',
        help='Output file path (overrides config file setting)'
    )
    
    parser.add_argument(
        '--log-level', '-l',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--log-file',
        help='Log file path (default: console only)'
    )
    
    parser.add_argument(
        '--create-example',
        action='store_true',
        help='Create an example configuration file'
    )
    
    parser.add_argument(
        '--validate-config',
        action='store_true',
        help='Validate configuration file and exit'
    )
    
    parser.add_argument(
        '--clear-state',
        help='Clear incremental state for specified source (or "all" for all sources)'
    )
    
    parser.add_argument(
        '--state-info',
        action='store_true',
        help='Show current state information'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='Market Monitor 2.0.0'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level, args.log_file)
    logger = logging.getLogger(__name__)
    
    try:
        # Create example config
        if args.create_example:
            if not args.config:
                print("Error: Please specify output file for example config")
                return 1
            
            config_data = create_example_config()
            
            config_path = Path(args.config)
            if config_path.suffix.lower() in ('.yml', '.yaml'):
                import yaml
                config_path.write_text(yaml.dump(config_data, indent=2))
            else:
                config_path.write_text(json.dumps(config_data, indent=2))
            
            print(f"Example configuration created: {args.config}")
            return 0
        
        # Require config file for other operations
        if not args.config:
            print("Error: Configuration file is required")
            parser.print_help()
            return 1
        
        if not Path(args.config).exists():
            print(f"Error: Configuration file not found: {args.config}")
            return 1
        
        # Create pipeline
        logger.info(f"Loading configuration from {args.config}")
        pipeline = Pipeline.from_config_file(args.config)
        
        # Override output path if specified
        if args.output:
            pipeline.config.output = args.output
        
        # Validate config only
        if args.validate_config:
            logger.info("Configuration is valid")
            print("✓ Configuration is valid")
            return 0
        
        # Show state info
        if args.state_info:
            state_info = pipeline.get_state_info()
            print("State Information:")
            for key, value in state_info.items():
                print(f"  {key}: {value}")
            return 0
        
        # Clear state
        if args.clear_state:
            source_name = args.clear_state if args.clear_state != 'all' else None
            pipeline.clear_state(source_name)
            
            if source_name:
                logger.info(f"Cleared state for source: {source_name}")
                print(f"✓ Cleared state for source: {source_name}")
            else:
                logger.info("Cleared state for all sources")
                print("✓ Cleared state for all sources")
            return 0
        
        # Run pipeline
        logger.info("Starting pipeline execution")
        logger.info(f"Sources: {len(pipeline.config.sources)}")
        logger.info(f"Max concurrent: {pipeline.config.max_concurrent}")
        logger.info(f"Global timeout: {pipeline.config.global_timeout}s")
        logger.info(f"Output: {pipeline.config.output}")
        
        result = await pipeline.run()
        
        # Check for pipeline error
        if 'error' in result:
            logger.error(f"Pipeline failed: {result['error']}")
            print(f"✗ Pipeline failed: {result['error']}")
            return 1
        
        # Print summary
        stats = result['stats']
        summary = {
            'total_time_ms': stats.get('total_time_ms', 0),
            'sources_total': len(stats.get('sources', [])),
            'sources_successful': len([s for s in stats.get('sources', []) if s.get('status') == 'success']),
            'sources_failed': len([s for s in stats.get('sources', []) if s.get('status') in ('failed', 'timeout')]),
            'records_final': stats.get('total_records', 0),
            'duplicates_removed': stats.get('duplicates_removed', 0)
        }
        
        print("\n✓ Pipeline completed successfully!")
        print(f"  Time: {summary['total_time_ms']:.0f}ms")
        print(f"  Sources: {summary['sources_successful']}/{summary['sources_total']} successful")
        
        if summary['sources_failed'] > 0:
            print(f"  Failed sources: {summary['sources_failed']}")
        
        print(f"  Records: {summary['records_final']} (removed {summary['duplicates_removed']} duplicates)")
        print(f"  Output: {pipeline.config.output}")
        
        logger.info("Pipeline execution completed successfully")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        print("\n✗ Pipeline interrupted")
        return 130
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        print(f"✗ Pipeline failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)