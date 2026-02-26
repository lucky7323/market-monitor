"""Fetchers package for multi-source data collection."""

from fetchers.base import BaseFetcher, FetchResult
from fetchers.rest import RestFetcher
from fetchers.csv_fetcher import CsvFetcher
from fetchers.websocket import WebSocketFetcher
from fetchers.graphql import GraphQLFetcher
from fetchers.file import FileFetcher

__all__ = [
    'BaseFetcher',
    'FetchResult',
    'RestFetcher',
    'CsvFetcher', 
    'WebSocketFetcher',
    'GraphQLFetcher',
    'FileFetcher',
]