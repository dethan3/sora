import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.data.fetcher import FundData, HistoricalData
from src.data.cache import DataCache


def test_save_and_get_fund_data_roundtrip(tmp_cache_dir: Path):
    cache = DataCache(cache_dir=str(tmp_cache_dir), expire_hours=24)
    fd = FundData(
        code="510300",
        name="沪深300ETF",
        current_price=3.21,
        previous_close=3.11,
        change_percent=3.21,
        volume=123456,
        market_cap=None,
        currency="CNY",
        last_update=datetime.now(),
    )

    assert cache.save_fund_data("510300", fd) is True

    got = cache.get_fund_data("510300")
    assert got is not None
    assert got.code == fd.code
    assert got.name == fd.name
    assert pytest.approx(got.current_price) == fd.current_price


def test_save_and_get_historical_roundtrip(tmp_cache_dir: Path):
    cache = DataCache(cache_dir=str(tmp_cache_dir), expire_hours=24)

    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2024-01-01", "2024-01-02"]),
            "Open": [1.0, 1.1],
            "High": [1.2, 1.3],
            "Low": [0.9, 1.0],
            "Close": [1.05, 1.15],
            "Volume": [1000, 1100],
        }
    ).set_index("Date")

    hd = HistoricalData(
        code="510300",
        data=df,
        start_date=df.index[0].to_pydatetime(),
        end_date=df.index[-1].to_pydatetime(),
        period="60d",
    )

    assert cache.save_historical_data("510300", hd) is True

    got_hd = cache.get_cached_historical_data("510300", period="60d")
    assert got_hd is not None
    assert got_hd.code == "510300"
    assert list(got_hd.data.columns) == list(df.columns)


def test_analysis_result_io(tmp_cache_dir: Path):
    cache = DataCache(cache_dir=str(tmp_cache_dir), expire_hours=24)

    result = {"score": 0.85, "signal": "buy"}
    assert cache.save_analysis_result("510300", result) is True

    got = cache.get_analysis_result("510300")
    assert got is not None
    assert got["signal"] == "buy"


def test_cleanup_expired_data(tmp_cache_dir: Path):
    # Use very small expire to force cleanup
    cache = DataCache(cache_dir=str(tmp_cache_dir), expire_hours=0)

    # Create a current cache file
    fd = FundData(
        code="510500",
        name="中证500ETF",
        current_price=5.0,
        previous_close=4.9,
        change_percent=2.0,
        volume=100,
    )
    assert cache.save_fund_data("510500", fd)

    # Make file older than expire by modifying mtime
    file_path = cache._get_cache_file_path("current", "510500")
    old_time = (datetime.now() - timedelta(hours=2)).timestamp()
    os.utime(file_path, (old_time, old_time))

    cleared = cache.cleanup_expired_data()
    assert cleared["current"] >= 1
