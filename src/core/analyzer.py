"""
核心分析模块 - MVP 版本

实现 ETFAnalyzer：
- 仅关注国内 ETF（China market）
- 依据“低位均值折价 ≥ buy_threshold(默认20%)”产出买入信号计数
- 返回 main.display_analysis_summary() 期望的摘要结构
"""
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
from loguru import logger

from src.config.manager import ConfigManager
from src.data.fetcher import DataFetcher, HistoricalData as FetcherHistorical
from src.analytics.calculator import AnalyticsCalculator


class ETFAnalyzer:
    """ETF 分析器（MVP）

    目标：提供 analyze_all/analyze_single 接口，产出摘要数据：
        - total_etfs
        - successful_fetches
        - buy_signals
        - sell_signals (MVP 暂不实现卖出规则，计 0)
        - analysis_time (str)
    """

    def __init__(self, config_dir: str = "config") -> None:
        self.config = ConfigManager(config_dir=config_dir)
        # 从策略配置读取低位均值参数
        lm_cfg = self.config.strategy.low_mean
        self.calc = AnalyticsCalculator(
            analysis_days=max(60, lm_cfg.min_required_days),
            lm_rolling_mean_days=lm_cfg.rolling_mean_days,
            lm_windows=lm_cfg.windows,
            lm_min_required_days=lm_cfg.min_required_days,
            lm_use_ema_fallback=lm_cfg.use_ema_fallback,
            lm_ema_days=lm_cfg.ema_days,
        )
        self.buy_threshold = lm_cfg.buy_threshold
        self.strong_buy_threshold = lm_cfg.strong_buy_threshold

        # 仅关注国内 ETF 代码
        self.target_etfs: List[Tuple[str, str]] = self._collect_china_etfs()
        logger.info(f"MVP Analyzer 目标ETF数量: {len(self.target_etfs)}")

        # 数据获取器（针对国内 ETF 使用 AKShare）
        self.fetcher = DataFetcher(
            fund_codes=[code for code, _ in self.target_etfs],
            request_timeout=self.config.data_source.akshare_timeout,
            max_retries=self.config.data_source.akshare_max_retries,
            batch_size=self.config.data_source.batch_size,
            rate_limit_delay=self.config.data_source.akshare_rate_limit_delay,
            cache_dir=self.config.system.cache_dir,
        )

    # ------------------------ 公共接口 ------------------------
    def analyze_all(self, force_update: bool = False) -> Dict[str, object]:
        codes = [c for c, _ in self.target_etfs]
        total = len(codes)
        if total == 0:
            return self._summary(total, 0, 0, 0)

        # 批量获取当前价格（作为现价）
        current_map = self.fetcher.batch_get_current_data(codes)
        successful_fetches = len(current_map)

        buy_signals = 0
        sell_signals = 0  # MVP 未实现

        # 对成功获取到现价的标的再拉历史数据并计算低位均值折价
        if successful_fetches:
            hist_map = self.fetcher.batch_get_historical_data(fund_codes=list(current_map.keys()), period="180d")
            for code, fund_data in current_map.items():
                try:
                    hd: Optional[FetcherHistorical] = hist_map.get(code)
                    if hd is None or hd.data is None or hd.data.empty:
                        continue
                    # 价格序列与当前价
                    prices = hd.data["Close"].astype(float).values
                    if prices.size == 0:
                        continue
                    current_price = float(fund_data.current_price) if fund_data.current_price is not None else float(prices[-1])
                    # 索引序列（用递增整数即可满足 _compute_low_mean_discount 的签名）
                    idx = list(range(len(prices)))
                    lm = self.calc._compute_low_mean_discount(prices, idx, current_price)
                    discount = lm.get("discount_ratio")
                    window_used = lm.get("window_used")
                    if discount is None:
                        continue
                    # 触发阈值：≥ buy_threshold 计买入
                    if discount >= self.buy_threshold and window_used not in ("insufficient", None):
                        buy_signals += 1
                except Exception as e:
                    logger.warning(f"低位均值计算失败: {code} - {e}")
                    continue

        return self._summary(total, successful_fetches, buy_signals, sell_signals)

    def analyze_single(self, etf_code: str, force_update: bool = False) -> Dict[str, object]:
        # 若单个不在目标清单中，仍尝试分析（名称占位）
        name = next((n for c, n in self.target_etfs if c == etf_code), etf_code)
        current = self.fetcher.get_current_data(etf_code)
        if not current:
            return self._summary(1, 0, 0, 0)

        hd = self.fetcher.get_historical_data(etf_code, period="180d")
        if not hd or hd.data is None or hd.data.empty:
            return self._summary(1, 1, 0, 0)

        try:
            prices = hd.data["Close"].astype(float).values
            if prices.size == 0:
                return self._summary(1, 1, 0, 0)
            current_price = float(current.current_price) if current.current_price is not None else float(prices[-1])
            idx = list(range(len(prices)))
            lm = self.calc._compute_low_mean_discount(prices, idx, current_price)
            discount = lm.get("discount_ratio")
            window_used = lm.get("window_used")
            buy_signals = 1 if (discount is not None and discount >= self.buy_threshold and window_used not in ("insufficient", None)) else 0
            return self._summary(1, 1, buy_signals, 0)
        except Exception as e:
            logger.warning(f"单标的低位均值计算失败: {etf_code} - {e}")
            return self._summary(1, 1, 0, 0)

    # ------------------------ 内部工具 ------------------------
    def _collect_china_etfs(self) -> List[Tuple[str, str]]:
        """从配置中收集中国市场 ETF 代码及名称。"""
        etfs: List[Tuple[str, str]] = []
        # priority_funds
        for f in self.config.priority_funds:
            if getattr(f, "region", "") == "china_market" and getattr(f, "enabled", True):
                # 仅收集形如 6 位数字的本地代码
                if str(f.code).isdigit():
                    etfs.append((str(f.code), f.name))
        # region_funds.china_market
        for f in self.config.region_funds.get("china_market", []):
            if getattr(f, "enabled", True) and str(f.code).isdigit():
                etfs.append((str(f.code), f.name))
        # 去重，保持顺序
        seen = set()
        uniq: List[Tuple[str, str]] = []
        for code, name in etfs:
            if code not in seen:
                uniq.append((code, name))
                seen.add(code)
        # 只取前若干只，避免首次全量过慢
        max_funds = min(len(uniq), 50)
        return uniq[:max_funds]

    def _summary(self, total: int, ok: int, buy: int, sell: int) -> Dict[str, object]:
        return {
            "total_etfs": int(total),
            "successful_fetches": int(ok),
            "buy_signals": int(buy),
            "sell_signals": int(sell),
            "analysis_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
