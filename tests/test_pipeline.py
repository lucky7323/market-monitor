"""Basic tests for the pipeline."""
import asyncio
import unittest
from unittest.mock import patch, MagicMock
import tempfile
import json
import os

from pipeline.core import Pipeline
from pipeline.models import SourceConfig, SourceType
from pipeline.incremental import IncrementalTracker


class TestPipeline(unittest.TestCase):
    """Test cases for Pipeline class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create test CSV file
        self.csv_file = os.path.join(self.temp_dir, "test.csv")
        with open(self.csv_file, 'w') as f:
            f.write("timestamp,value,unit\n")
            f.write("2026-02-26T10:00:00Z,100.5,USD\n")
            f.write("2026-02-26T10:01:00Z,101.2,USD\n")
        
        # Create test JSON file
        self.json_file = os.path.join(self.temp_dir, "test.json")
        with open(self.json_file, 'w') as f:
            json.dump([
                {"timestamp": "2026-02-26T10:00:00Z", "value": 250.5, "unit": "EUR"},
                {"timestamp": "2026-02-26T10:01:00Z", "value": 251.2, "unit": "EUR"}
            ], f)
    
    def test_basic_pipeline(self):
        """Test basic pipeline functionality."""
        sources = [
            SourceConfig(
                name="test_csv",
                type=SourceType.CSV,
                path=self.csv_file
            ),
            SourceConfig(
                name="test_json", 
                type=SourceType.FILE,
                path=self.json_file
            )
        ]
        
        pipeline = Pipeline(sources)
        
        # Run pipeline
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(pipeline.process())
            
            # Check results
            self.assertEqual(result.stats.total_sources, 2)
            self.assertEqual(result.stats.successful_sources, 2)
            self.assertEqual(len(result.stats.failed_sources), 0)
            self.assertEqual(result.stats.total_records, 4)
            self.assertGreater(result.stats.records_per_second, 0)
            
        finally:
            loop.close()
    
    def test_incremental_tracker(self):
        """Test incremental tracking functionality."""
        tracker = IncrementalTracker(os.path.join(self.temp_dir, "state.json"))
        
        # Test initial state
        self.assertIsNone(tracker.get_last_update("test_source"))
        
        # Test updating state
        from datetime import datetime
        now = datetime.utcnow()
        tracker.update_last_update("test_source", now, 10)
        
        # Test retrieving state
        last_update = tracker.get_last_update("test_source")
        self.assertIsNotNone(last_update)
        self.assertEqual(last_update.replace(microsecond=0), now.replace(microsecond=0))
        
        # Test stats
        stats = tracker.get_stats()
        self.assertEqual(stats['tracked_sources'], 1)
        self.assertIn('test_source', stats['sources'])


if __name__ == '__main__':
    unittest.main()