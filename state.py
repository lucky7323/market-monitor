"""State management for incremental updates with backup and recovery."""

import json
import shutil
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Union


class StateStore:
    """Thread-safe state store for incremental updates."""
    
    VERSION = "1.0"
    
    def __init__(self, state_file: str = ".state.json"):
        """Initialize state store.
        
        Args:
            state_file: Path to state file
        """
        self.path = Path(state_file)
        self.lock = threading.RLock()
        
        # State structure:
        # {
        #   "version": "1.0",
        #   "sources": {
        #     "source_name": {
        #       "watermark": "last_value",
        #       "updated_at": "2024-01-01T00:00:00Z"
        #     }
        #   }
        # }
        self._state = {
            "version": self.VERSION,
            "sources": {}
        }
        
        # Pending watermarks (not yet committed)
        self._pending = {}
        
        self._load_or_create()
    
    def _load_or_create(self):
        """Load existing state or create new one."""
        if self.path.exists():
            try:
                self._load_state()
            except Exception as e:
                print(f"Warning: Failed to load state file {self.path}: {e}")
                self._try_restore_backup()
        else:
            self._save()
    
    def _load_state(self):
        """Load state from file with validation."""
        content = self.path.read_text(encoding='utf-8')
        loaded_state = json.loads(content)
        
        # Validate state structure
        if not isinstance(loaded_state, dict):
            raise ValueError("State file is not a JSON object")
        
        if "version" not in loaded_state:
            # Legacy state without version - migrate
            self._migrate_legacy_state(loaded_state)
        elif loaded_state["version"] != self.VERSION:
            # Version mismatch - attempt migration
            self._migrate_state(loaded_state)
        else:
            # Valid current version
            self._state = loaded_state
        
        # Ensure sources key exists
        if "sources" not in self._state:
            self._state["sources"] = {}
    
    def _migrate_legacy_state(self, old_state: dict):
        """Migrate legacy state format to current version."""
        print(f"Migrating legacy state file to version {self.VERSION}")
        
        # Legacy format might be: {"source_name": "watermark"}
        sources = {}
        for key, value in old_state.items():
            if isinstance(value, (str, int, float)):
                sources[key] = {
                    "watermark": value,
                    "updated_at": datetime.utcnow().isoformat()
                }
        
        self._state = {
            "version": self.VERSION,
            "sources": sources
        }
    
    def _migrate_state(self, old_state: dict):
        """Migrate state from different version."""
        # For now, just preserve sources and update version
        self._state = {
            "version": self.VERSION,
            "sources": old_state.get("sources", {})
        }
    
    def _try_restore_backup(self):
        """Try to restore from backup file."""
        backup_path = self.path.with_suffix(".bak")
        if backup_path.exists():
            try:
                print(f"Attempting to restore from backup: {backup_path}")
                backup_content = backup_path.read_text(encoding='utf-8')
                self._state = json.loads(backup_content)
                
                # Validate backup
                if "sources" not in self._state:
                    self._state["sources"] = {}
                
                print("Successfully restored from backup")
                self._save()  # Save the restored state
                return
            except Exception as e:
                print(f"Failed to restore backup: {e}")
        
        # If backup restore fails, start with empty state
        print("Starting with empty state")
        self._state = {"version": self.VERSION, "sources": {}}
        self._save()
    
    def get_watermark(self, source: str) -> Optional[Any]:
        """Get the last processed watermark for a source."""
        with self.lock:
            source_data = self._state["sources"].get(source)
            if source_data and isinstance(source_data, dict):
                return source_data.get("watermark")
            return None
    
    def stage_watermark(self, source: str, value: Any):
        """Stage a watermark for later commit.
        
        This is called during normalization to track the max value seen.
        The watermark is not persisted until commit() is called.
        """
        with self.lock:
            current = self._pending.get(source)
            
            # Only update if this is a newer value
            try:
                if current is None or self._is_newer_value(value, current):
                    self._pending[source] = value
            except Exception:
                # If comparison fails, always update (safer)
                self._pending[source] = value
    
    def _is_newer_value(self, new_value: Any, current_value: Any) -> bool:
        """Compare values to determine if new_value is newer."""
        # Handle different types
        if type(new_value) != type(current_value):
            return True  # Different types, assume newer
        
        if isinstance(new_value, (int, float)):
            return new_value > current_value
        elif isinstance(new_value, str):
            return new_value > current_value
        else:
            # For other types, convert to string and compare
            return str(new_value) > str(current_value)
    
    def commit(self, successful_sources: set[str], partial_sources: set[str]):
        """Commit staged watermarks for successful and partial sources.
        
        Commit policy:
        - success: Save pending watermark
        - partial: Save pending watermark (received data is valid)
        - failed: Keep previous watermark (retry on next run)
        """
        with self.lock:
            sources_to_update = successful_sources | partial_sources
            
            for source in sources_to_update:
                watermark = self._pending.get(source)
                if watermark is not None:
                    # Ensure source entry exists
                    if source not in self._state["sources"]:
                        self._state["sources"][source] = {}
                    
                    # Update watermark and timestamp
                    self._state["sources"][source]["watermark"] = watermark
                    self._state["sources"][source]["updated_at"] = datetime.utcnow().isoformat()
            
            # Clear all pending watermarks
            self._pending.clear()
            
            # Persist changes
            self._save()
    
    def _save(self):
        """Atomically save state with backup."""
        with self.lock:
            # Create backup of existing state
            if self.path.exists():
                backup_path = self.path.with_suffix(".bak")
                try:
                    shutil.copy2(self.path, backup_path)
                except Exception as e:
                    print(f"Warning: Failed to create backup: {e}")
            
            # Atomic write: write to temp file, then rename
            temp_path = self.path.with_suffix(".tmp")
            try:
                temp_path.write_text(
                    json.dumps(self._state, indent=2, ensure_ascii=False),
                    encoding='utf-8'
                )
                temp_path.rename(self.path)
            except Exception as e:
                # Clean up temp file if it exists
                if temp_path.exists():
                    temp_path.unlink()
                raise e
    
    def clear(self, source: Optional[str] = None):
        """Clear state for a source or all sources."""
        with self.lock:
            if source:
                self._state["sources"].pop(source, None)
                self._pending.pop(source, None)
            else:
                self._state["sources"].clear()
                self._pending.clear()
            
            self._save()
    
    def get_all_sources(self) -> dict[str, Any]:
        """Get state for all sources (read-only)."""
        with self.lock:
            return self._state["sources"].copy()
    
    def get_source_info(self, source: str) -> dict[str, Any]:
        """Get detailed info for a source."""
        with self.lock:
            source_data = self._state["sources"].get(source, {})
            pending = self._pending.get(source)
            
            return {
                "watermark": source_data.get("watermark"),
                "updated_at": source_data.get("updated_at"),
                "pending_watermark": pending,
                "has_pending": pending is not None
            }
    
    def health_check(self) -> dict[str, Any]:
        """Perform health check on state store."""
        with self.lock:
            return {
                "state_file_exists": self.path.exists(),
                "state_file_readable": self.path.is_file() if self.path.exists() else False,
                "backup_exists": self.path.with_suffix(".bak").exists(),
                "version": self._state.get("version"),
                "source_count": len(self._state["sources"]),
                "pending_count": len(self._pending),
                "state_file_size": self.path.stat().st_size if self.path.exists() else 0
            }


# Export main class
__all__ = ['StateStore']