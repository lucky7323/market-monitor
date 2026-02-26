"""Incremental update functionality for the pipeline."""
import json
import os
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path

from .models import SourceConfig, Record


class IncrementalTracker:
    """Tracks incremental state for sources."""
    
    def __init__(self, state_file: str = "incremental_state.json"):
        """Initialize with state file path.
        
        Args:
            state_file: Path to state file for tracking incremental updates
        """
        self.state_file = Path(state_file)
        self.state = self._load_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """Load state from file."""
        if self.state_file.exists():
            with open(self.state_file, 'r') as f:
                return json.load(f)
        return {}
    
    def _save_state(self):
        """Save state to file."""
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2, default=str)
    
    def get_last_update(self, source_name: str) -> Optional[datetime]:
        """Get last update timestamp for a source.
        
        Args:
            source_name: Name of the source
            
        Returns:
            datetime: Last update timestamp, None if never updated
        """
        source_state = self.state.get(source_name, {})
        last_update = source_state.get('last_update')
        if last_update:
            return datetime.fromisoformat(last_update)
        return None
    
    def update_last_update(self, source_name: str, timestamp: datetime, record_count: int):
        """Update last update timestamp for a source.
        
        Args:
            source_name: Name of the source
            timestamp: New last update timestamp
            record_count: Number of records processed
        """
        if source_name not in self.state:
            self.state[source_name] = {}
        
        self.state[source_name].update({
            'last_update': timestamp.isoformat(),
            'last_record_count': record_count,
            'update_history': self.state[source_name].get('update_history', [])
        })
        
        # Keep last 10 update history entries
        history = self.state[source_name]['update_history']
        history.append({
            'timestamp': timestamp.isoformat(),
            'record_count': record_count
        })
        self.state[source_name]['update_history'] = history[-10:]
        
        self._save_state()
    
    def get_incremental_filter(self, source_config: SourceConfig) -> Optional[Dict[str, Any]]:
        """Get filter parameters for incremental updates.
        
        Args:
            source_config: Source configuration
            
        Returns:
            Dict: Filter parameters to add to requests
        """
        last_update = self.get_last_update(source_config.name)
        if not last_update:
            return None
        
        # Return filter based on source type
        if source_config.type.value in ['rest', 'graphql']:
            return {
                'since': last_update.isoformat(),
                'updated_after': last_update.isoformat()
            }
        elif source_config.type.value == 'csv':
            # For CSV files, we could check file modification time
            if source_config.path and os.path.exists(source_config.path):
                file_mtime = datetime.fromtimestamp(os.path.getmtime(source_config.path))
                if file_mtime <= last_update:
                    # File hasn't changed, skip
                    return {'skip': True}
        
        return None
    
    def should_skip_source(self, source_config: SourceConfig) -> bool:
        """Check if source should be skipped for incremental update.
        
        Args:
            source_config: Source configuration
            
        Returns:
            bool: True if source should be skipped
        """
        filter_params = self.get_incremental_filter(source_config)
        return filter_params and filter_params.get('skip', False)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get incremental tracking statistics.
        
        Returns:
            Dict: Statistics about tracked sources
        """
        return {
            'tracked_sources': len(self.state),
            'sources': {
                name: {
                    'last_update': state.get('last_update'),
                    'last_record_count': state.get('last_record_count', 0),
                    'update_count': len(state.get('update_history', []))
                }
                for name, state in self.state.items()
            }
        }