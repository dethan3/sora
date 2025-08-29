from datetime import datetime

import pandas as pd
import pytest

from src.data.fetcher import DataFetcher, FundData


@pytest.fixture()
def fetcher(tmp_path):
    return DataFetcher(cache_dir=str(tmp_path / "cache"))


def _etf_spot_df_row(code: str,
                     name: str = "MockETF",
                     latest: float = 1.23,
                     prev: float = 1.00,
                     pct: float = 23.0,
                     vol: int = 1000):
    return {
        "代码": code,
        "名称": name,
        "最新价": latest,
        "昨收": prev,
        "涨跌幅": pct,
        "成交量": vol,
    }


def test_get_current_data_uses_cached_etf_list(fetcher, mocker):
    df = pd.DataFrame([_etf_spot_df_row("510300", "沪深300ETF", 3.21, 3.11, 3.21, 123456)])
    mocker.patch.object(DataFetcher, "_get_etf_list_cached", return_value=df)

    data = fetcher.get_current_data("510300")
    assert isinstance(data, FundData)
    assert data.code == "510300"
    assert data.name == "沪深300ETF"
    assert pytest.approx(data.current_price) == 3.21


def test_batch_get_current_data_from_cache(fetcher, mocker):
    rows = [
        _etf_spot_df_row("510300", "沪深300ETF", 3.21, 3.11, 3.21, 123456),
        _etf_spot_df_row("510500", "中证500ETF", 5.01, 4.98, 0.6, 8888),
    ]
    df = pd.DataFrame(rows)
    mocker.patch.object(DataFetcher, "_get_etf_list_cached", return_value=df)

    res = fetcher.batch_get_current_data(["510300", "510500"])  # both valid
    assert set(res.keys()) == {"510300", "510500"}
    assert all(isinstance(v, FundData) for v in res.values())


def test_get_historical_data_from_cache(fetcher, mocker):
    # mock cached df
    dates = pd.to_datetime(["2024-01-01", "2024-01-02"])    
    df = pd.DataFrame({
        "Open": [1.0, 1.1],
        "High": [1.2, 1.3],
        "Low": [0.9, 1.0],
        "Close": [1.05, 1.15],
        "Volume": [1000, 1100],
    }, index=dates)

    mocker.patch.object(DataFetcher, "_get_cached_historical_data", return_value=df)

    hd = fetcher.get_historical_data("510300", period="60d")
    assert hd is not None
    assert hd.code == "510300"
    assert list(hd.data.columns) == ["Open", "High", "Low", "Close", "Volume"]


def test_get_historical_data_fetch_and_process(fetcher, mocker):
    # Mock akshare min hist (Chinese columns)
    cn_df = pd.DataFrame({
        "时间": ["2024-01-01 09:35", "2024-01-01 09:40"],
        "开盘": [1.0, 1.1],
        "收盘": [1.05, 1.15],
        "最高": [1.2, 1.3],
        "最低": [0.9, 1.0],
        "成交量": [1000, 1100],
    })
    mocker.patch("src.data.fetcher.ak.fund_etf_hist_min_em", return_value=cn_df)
    # ensure cache miss so it goes to API path
    mocker.patch.object(DataFetcher, "_get_cached_historical_data", return_value=None)
    # do not write parquet during test
    mocker.patch.object(DataFetcher, "_save_historical_data_to_cache", return_value=True)

    hd = fetcher.get_historical_data("510300", period="60d")
    assert hd is not None
    # columns processed to EN names with Date index
    assert set(["Open", "High", "Low", "Close", "Volume"]).issubset(hd.data.columns)
