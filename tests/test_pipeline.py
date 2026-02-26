"""Basic tests for the pipeline."""
import asyncio
import unittest
import tempfile
import json
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config import PipelineConfig, SourceConfig
from pipeline import Pipeline
from state import StateStore


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
    
    def test_config_validation(self):
        """Test that config validation works for file sources."""
        config_data = {
            "sources": [
                {
                    "name": "test_csv",
                    "type": "csv",
                    "path": self.csv_file
                },
                {
                    "name": "test_json",
                    "type": "file",
                    "path": self.json_file
                }
            ],
            "output": os.path.join(self.temp_dir, "output.json"),
            "allowed_input_dirs": [self.temp_dir],
            "allowed_output_dirs": [self.temp_dir]
        }
        
        config = PipelineConfig.model_validate(config_data)
        assert len(config.sources) == 2
        assert config.sources[0].type == "csv"
        assert config.sources[1].type == "file"
    
    def test_state_store_operations(self):
        """Test state store basic operations."""
        state_file = os.path.join(self.temp_dir, "state.json")
        state = StateStore(state_file)
        
        # Test initial state
        assert state.get_watermark("test_source") is None
        
        # Test staging and committing
        state.stage_watermark("test_source", "2026-01-01T00:00:00Z")
        state.commit({"test_source"}, set())
        
        assert state.get_watermark("test_source") == "2026-01-01T00:00:00Z"


if __name__ == '__main__':
    unittest.main()
