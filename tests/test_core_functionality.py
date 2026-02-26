"""Tier 1 core functionality tests - Critical regression prevention."""

import asyncio
import pytest
import tempfile
import json
from pathlib import Path

from config import PipelineConfig, SourceConfig
from pipeline import Pipeline
from state import StateStore
from security import validate_url_format, validate_file_path
from stats import StatsCollector, SourceStats
from merger import Merger
from normalizer import Normalizer


class TestConfigValidation:
    """Test configuration validation and parsing."""
    
    def test_valid_rest_config(self):
        """Test valid REST source configuration."""
        config_data = {
            "sources": [{
                "name": "test_api",
                "type": "rest",
                "url": "https://api.example.com/data",
                "timeout": 10.0
            }]
        }
        
        config = PipelineConfig.model_validate(config_data)
        assert len(config.sources) == 1
        assert config.sources[0].name == "test_api"
        assert config.sources[0].type == "rest"
        assert config.sources[0].url == "https://api.example.com/data"
    
    def test_invalid_source_type(self):
        """Test rejection of invalid source types."""
        config_data = {
            "sources": [{
                "name": "test",
                "type": "invalid_type",
                "url": "https://example.com"
            }]
        }
        
        with pytest.raises(ValueError):
            PipelineConfig.model_validate(config_data)
    
    def test_missing_required_fields(self):
        """Test validation of type-specific required fields."""
        # REST without URL should fail
        config_data = {
            "sources": [{
                "name": "test",
                "type": "rest"
                # Missing url
            }]
        }
        
        with pytest.raises(ValueError, match="requires 'url'"):
            PipelineConfig.model_validate(config_data)
    
    def test_duplicate_source_names(self):
        """Test rejection of duplicate source names."""
        config_data = {
            "sources": [
                {"name": "duplicate", "type": "rest", "url": "https://example.com"},
                {"name": "duplicate", "type": "rest", "url": "https://example2.com"}
            ]
        }
        
        with pytest.raises(ValueError, match="Source names must be unique"):
            PipelineConfig.model_validate(config_data)


class TestSecurityValidation:
    """Test security validation functions."""
    
    def test_valid_urls(self):
        """Test acceptance of valid URLs."""
        valid_urls = [
            "https://api.example.com/data",
            "http://public.api.com/endpoint",
            "wss://websocket.example.com/feed"
        ]
        
        for url in valid_urls:
            validate_url_format(url)  # Should not raise
    
    def test_invalid_url_schemes(self):
        """Test rejection of dangerous URL schemes."""
        invalid_urls = [
            "ftp://example.com/file",
            "file:///etc/passwd",
            "javascript:alert(1)"
        ]
        
        for url in invalid_urls:
            with pytest.raises(ValueError, match="Disallowed scheme"):
                validate_url_format(url)
    
    def test_path_validation(self):
        """Test file path validation."""
        # Valid paths
        validate_file_path("./data/test.json")
        validate_file_path("data.csv")
        
        # Should work with relative paths
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test.json"
            test_file.write_text("{}")
            validate_file_path(str(test_file), [tmpdir])


class TestStateManagement:
    """Test incremental state management."""
    
    def test_state_store_basic_operations(self):
        """Test basic state store operations."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            state_file = f.name
        
        try:
            # Create state store
            state = StateStore(state_file)
            
            # Test watermark operations
            assert state.get_watermark("test_source") is None
            
            state.stage_watermark("test_source", "2024-01-01T00:00:00Z")
            state.commit({"test_source"}, set())
            
            assert state.get_watermark("test_source") == "2024-01-01T00:00:00Z"
            
        finally:
            Path(state_file).unlink(missing_ok=True)
    
    def test_state_commit_policies(self):
        """Test different commit policies for success/partial/failed."""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            state_file = f.name
        
        try:
            state = StateStore(state_file)
            
            # Stage watermarks for multiple sources
            state.stage_watermark("success_source", "100")
            state.stage_watermark("partial_source", "200") 
            state.stage_watermark("failed_source", "300")
            
            # Commit with different policies
            state.commit({"success_source"}, {"partial_source"})  # failed_source not included
            
            # Check results
            assert state.get_watermark("success_source") == "100"
            assert state.get_watermark("partial_source") == "200"
            assert state.get_watermark("failed_source") is None  # Should not be saved
            
        finally:
            Path(state_file).unlink(missing_ok=True)


class TestDataNormalization:
    """Test data normalization functionality."""
    
    def test_basic_normalization(self):
        """Test basic record normalization."""
        normalizer = Normalizer()
        
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            state_file = f.name
        
        try:
            state = StateStore(state_file)
            source_config = SourceConfig(
                name="test_source",
                type="rest",
                url="https://example.com"
            )
            
            raw_records = [
                {"timestamp": "2024-01-01T00:00:00Z", "value": 100, "unit": "USD"},
                {"timestamp": "2024-01-01T01:00:00Z", "value": 200, "unit": "USD"}
            ]
            
            normalized = normalizer.normalize(raw_records, source_config, state)
            
            assert len(normalized) == 2
            assert all("source" in record for record in normalized)
            assert all(record["source"] == "test_source" for record in normalized)
            
        finally:
            Path(state_file).unlink(missing_ok=True)
    
    def test_watermark_staging(self):
        """Test watermark staging during normalization."""
        normalizer = Normalizer()
        
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            state_file = f.name
        
        try:
            state = StateStore(state_file)
            source_config = SourceConfig(
                name="test_source",
                type="rest", 
                url="https://example.com",
                incremental=True,
                incremental_field="timestamp"
            )
            
            raw_records = [
                {"timestamp": "2024-01-01T00:00:00Z", "value": 100},
                {"timestamp": "2024-01-01T02:00:00Z", "value": 200},  # Later timestamp
                {"timestamp": "2024-01-01T01:00:00Z", "value": 150}   # Middle timestamp
            ]
            
            normalized = normalizer.normalize(raw_records, source_config, state)
            
            # Check that max timestamp was staged
            source_info = state.get_source_info("test_source")
            assert source_info["pending_watermark"] == "2024-01-01T02:00:00Z"
            
        finally:
            Path(state_file).unlink(missing_ok=True)


class TestDataMerging:
    """Test data merging and deduplication."""
    
    def test_deduplication(self):
        """Test duplicate record removal."""
        merger = Merger()
        
        records = [
            {"source": "test", "timestamp": "2024-01-01T00:00:00Z", "value": 100},
            {"source": "test", "timestamp": "2024-01-01T00:00:00Z", "value": 100},  # Duplicate
            {"source": "test", "timestamp": "2024-01-01T01:00:00Z", "value": 200}
        ]
        
        merged = merger.merge(records)
        
        assert len(merged) == 2  # One duplicate removed
    
    def test_sorting(self):
        """Test record sorting by timestamp."""
        merger = Merger()
        
        records = [
            {"source": "test", "timestamp": "2024-01-01T02:00:00Z", "value": 300},
            {"source": "test", "timestamp": "2024-01-01T00:00:00Z", "value": 100},
            {"source": "test", "timestamp": "2024-01-01T01:00:00Z", "value": 200}
        ]
        
        merged = merger.merge(records)
        
        # Should be sorted by timestamp
        timestamps = [record["timestamp"] for record in merged]
        assert timestamps == sorted(timestamps)


class TestStatsCollection:
    """Test statistics collection."""
    
    def test_stats_collector_basic(self):
        """Test basic statistics collection."""
        from fetchers.base import FetchResult
        
        collector = StatsCollector()
        
        # Simulate successful fetch
        fetch_result = FetchResult(
            source_name="test_source",
            records=[{"value": 1}, {"value": 2}],
            fetch_time_ms=150.0
        )
        
        normalized_records = [{"source": "test_source", "value": 1}]  # One lost in normalization
        
        collector.add_source_result("test_source", fetch_result, normalized_records)
        
        stats = collector.finish()
        
        assert len(stats.sources) == 1
        source_stats = stats.sources[0]
        assert source_stats.name == "test_source"
        assert source_stats.status == "success"
        assert source_stats.records_fetched == 2
        assert source_stats.records_after_normalize == 1
        assert source_stats.fetch_time_ms == 150.0
    
    def test_stats_error_tracking(self):
        """Test error tracking in statistics."""
        from fetchers.base import FetchResult
        
        collector = StatsCollector()
        
        # Simulate failed fetch
        fetch_result = FetchResult(
            source_name="failed_source",
            error="Connection timeout",
            is_partial=True
        )
        
        collector.add_source_result("failed_source", fetch_result, [])
        
        stats = collector.finish()
        
        assert len(stats.failed_sources) == 0  # Partial, not failed
        assert len(stats.partial_sources) == 1
        assert stats.success_rate == 1.0  # Partial counts as success for rate


@pytest.mark.asyncio
class TestPipelineIntegration:
    """Test end-to-end pipeline functionality."""
    
    async def test_pipeline_with_mock_config(self):
        """Test pipeline with minimal mock configuration."""
        # Create minimal config for testing
        config_data = {
            "sources": [
                {
                    "name": "mock_rest",
                    "type": "rest",
                    "url": "https://httpbin.org/json",  # Public test API
                    "timeout": 5.0
                }
            ],
            "output": "test_output.json",
            "max_concurrent": 1,
            "global_timeout": 10.0
        }
        
        config = PipelineConfig.model_validate(config_data)
        pipeline = Pipeline(config)
        
        # This test might fail if network is unavailable, which is expected
        # The main goal is to test that the pipeline doesn't crash
        try:
            result = await pipeline.run()
            
            # Basic structure checks
            assert "records" in result
            assert "stats" in result
            assert isinstance(result["records"], list)
            assert isinstance(result["stats"], dict)
            
            # Clean up
            Path("test_output.json").unlink(missing_ok=True)
            
        except Exception as e:
            # Network errors are acceptable in unit tests
            if "network" in str(e).lower() or "connection" in str(e).lower():
                pytest.skip(f"Network not available: {e}")
            else:
                raise


if __name__ == "__main__":
    pytest.main([__file__, "-v"])