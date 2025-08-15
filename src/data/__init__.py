"""
数据访问层模块

提供基金数据获取、缓存和管理功能。
"""

from .fetcher import DataFetcher
from .cache import DataCache

__all__ = ['DataFetcher', 'DataCache']
