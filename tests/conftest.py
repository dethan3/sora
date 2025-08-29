import sys
from pathlib import Path
import os
import contextlib
import typing as t
import pytest

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Optional freezegun support
try:
    from freezegun import freeze_time as _freeze_time
except Exception:  # pragma: no cover
    _freeze_time = None

from src.data.cache import DataCache
from src.data.fetcher import DataFetcher


@pytest.fixture(scope="session")
def project_root() -> Path:
    return PROJECT_ROOT


@pytest.fixture()
def tmp_cache_dir(tmp_path: Path) -> Path:
    d = tmp_path / "cache"
    d.mkdir(parents=True, exist_ok=True)
    return d


@pytest.fixture()
def data_cache(tmp_cache_dir: Path) -> DataCache:
    # Short expiry for tests
    return DataCache(cache_dir=str(tmp_cache_dir), expire_hours=1, max_cache_size_mb=10)


@pytest.fixture()
@contextlib.contextmanager
def freeze_time():
    """Context manager fixture to freeze time if freezegun is installed.
    Usage:
        with freeze_time()("2024-01-01 00:00:00"):
            ...
    If freezegun isn't installed, returns a no-op contextmanager.
    """
    if _freeze_time is None:
        @contextlib.contextmanager
        def _noop(*args: t.Any, **kwargs: t.Any):
            yield
        yield _noop
    else:
        yield _freeze_time


# Integration test helper for tests/test_fetcher.py
@pytest.fixture()
def fetcher(tmp_cache_dir: Path) -> DataFetcher:
    # Do not set fund_codes here; tests pass codes explicitly.
    return DataFetcher(cache_dir=str(tmp_cache_dir))
