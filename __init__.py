"""Market Monitor - Multi-source data aggregation pipeline."""

__version__ = "2.0.0"
__author__ = "Market Monitor Team"
__description__ = "Async Python pipeline for multi-source data aggregation"

try:
    from .pipeline import Pipeline, run_pipeline_from_config
    from .config import PipelineConfig, SourceConfig, load_config, create_example_config
    from .state import StateStore
    from .stats import PipelineStats, SourceStats, StatsCollector

    __all__ = [
        'Pipeline',
        'PipelineConfig',
        'SourceConfig', 
        'StateStore',
        'PipelineStats',
        'SourceStats',
        'StatsCollector',
        'run_pipeline_from_config',
        'load_config',
        'create_example_config',
    ]
except ImportError:
    # Allow direct module imports when not used as a package
    pass
