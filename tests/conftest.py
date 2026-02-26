"""Test configuration - add parent directory to sys.path."""
import sys
import os

# Add market-monitor directory to path so tests can import modules directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
