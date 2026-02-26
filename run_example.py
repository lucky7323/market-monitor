#!/usr/bin/env python3
"""Simple example script to run the market monitor pipeline."""

import asyncio
import json
from pathlib import Path

# Import from our package
from pipeline import Pipeline
from config import create_example_config


async def run_simple_example():
    """Run a simple example with mock data."""
    print("🚀 Market Monitor V2 - Simple Example")
    print("=" * 50)
    
    # Create a simple test configuration
    config_data = {
        "sources": [
            {
                "name": "httpbin_test",
                "type": "rest",
                "url": "https://httpbin.org/json",
                "timeout": 5.0
            }
        ],
        "output": "example_output.json",
        "max_concurrent": 1,
        "global_timeout": 10.0,
        "allowed_hosts": ["httpbin.org"]
    }
    
    # Save config to file
    config_file = "example_config.json"
    with open(config_file, 'w') as f:
        json.dump(config_data, f, indent=2)
    
    print(f"📝 Created config: {config_file}")
    
    try:
        # Create and run pipeline
        pipeline = Pipeline.from_config_file(config_file)
        
        print("🔄 Running pipeline...")
        result = await pipeline.run()
        
        # Print results
        if 'error' in result:
            print(f"❌ Pipeline failed: {result['error']}")
            return
        
        stats = result['stats']
        print(f"✅ Pipeline completed successfully!")
        print(f"   Total time: {stats.get('total_time_ms', 0):.0f}ms")
        print(f"   Sources: {len(stats.get('sources', []))}")
        print(f"   Records: {stats.get('total_records', 0)}")
        print(f"   Output: {config_data['output']}")
        
        # Show first few records
        records = result['records']
        if records:
            print(f"\n📊 Sample records ({len(records)} total):")
            for i, record in enumerate(records[:3]):
                print(f"   {i+1}. {record}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("   This might be due to network connectivity issues.")
    
    finally:
        # Cleanup
        Path(config_file).unlink(missing_ok=True)
        Path("example_output.json").unlink(missing_ok=True)


async def create_full_example_config():
    """Create a full example configuration."""
    print("\n📋 Creating full example configuration...")
    
    config_data = create_example_config()
    
    # Write as YAML
    import yaml
    with open("config_example_full.yaml", 'w') as f:
        yaml.dump(config_data, f, indent=2, default_flow_style=False)
    
    print("✅ Created: config_example_full.yaml")
    print("   Edit this file with your actual data sources and run:")
    print("   python main.py config_example_full.yaml")


if __name__ == "__main__":
    print("Market Monitor V2 - Example Runner")
    print("Choose an option:")
    print("1. Run simple test with httpbin.org")
    print("2. Create full example configuration")
    
    choice = input("\nEnter choice (1 or 2): ").strip()
    
    if choice == "1":
        asyncio.run(run_simple_example())
    elif choice == "2":
        asyncio.run(create_full_example_config())
    else:
        print("Invalid choice. Run with option 1 or 2.")