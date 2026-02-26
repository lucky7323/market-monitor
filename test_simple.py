#!/usr/bin/env python3
"""Simple test for Market Monitor V2."""

import asyncio
import json
from pathlib import Path

from config import PipelineConfig
from pipeline import Pipeline


async def test_simple():
    """Test with httpbin.org (public API)."""
    print("🧪 Testing Market Monitor V2...")
    
    # Simple test config
    config_data = {
        "sources": [
            {
                "name": "httpbin_test", 
                "type": "rest",
                "url": "https://httpbin.org/json",
                "timeout": 10.0
            }
        ],
        "output": "test_output.json",
        "max_concurrent": 1,
        "global_timeout": 15.0,
        "allowed_hosts": ["httpbin.org"]
    }
    
    try:
        # Create pipeline
        config = PipelineConfig.model_validate(config_data)
        pipeline = Pipeline(config)
        
        print("⏳ Running pipeline...")
        result = await pipeline.run()
        
        if 'error' in result:
            print(f"❌ Error: {result['error']}")
            return False
        
        # Check results
        stats = result['stats']
        records = result['records']
        
        print(f"✅ Success!")
        print(f"   Time: {stats.get('total_time_ms', 0):.0f}ms")
        print(f"   Sources: {len(stats.get('sources', []))}")
        print(f"   Records: {len(records)}")
        
        if records:
            print(f"   Sample: {records[0]}")
        
        return True
        
    except Exception as e:
        import traceback
        print(f"❌ Exception: {e}")
        traceback.print_exc()
        return False
    
    finally:
        # Cleanup
        Path("test_output.json").unlink(missing_ok=True)
        Path(".state.json").unlink(missing_ok=True)


if __name__ == "__main__":
    success = asyncio.run(test_simple())
    exit(0 if success else 1)