"""
数据缓存模块

负责缓存历史数据，减少 API 调用，包括：
- 历史数据本地存储 (JSON/Pickle 格式)
- 缓存更新策略 (增量更新机制)
- 缓存失效检查
- 数据压缩和清理
"""

import os
import json
import pickle
import gzip
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
import pandas as pd
from loguru import logger

from .fetcher import FundData, HistoricalData


class DataCache:
    """数据缓存管理器"""
    
    def __init__(self, cache_dir: str = "data/cache", 
                 expire_hours: int = 24, 
                 max_cache_size_mb: int = 100):
        """
        初始化数据缓存
        
        Args:
            cache_dir: 缓存目录
            expire_hours: 缓存过期时间(小时)
            max_cache_size_mb: 最大缓存大小(MB)
        """
        self.cache_dir = Path(cache_dir)
        self.expire_hours = expire_hours
        self.max_cache_size_mb = max_cache_size_mb
        
        # 创建缓存目录
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # 缓存子目录
        self.current_data_dir = self.cache_dir / "current"
        self.historical_data_dir = self.cache_dir / "historical"
        self.fund_info_dir = self.cache_dir / "info"
        
        for dir_path in [self.current_data_dir, self.historical_data_dir, self.fund_info_dir]:
            dir_path.mkdir(exist_ok=True)
        
        logger.info(f"数据缓存初始化完成 - 目录:{cache_dir}, 过期:{expire_hours}h, 最大:{max_cache_size_mb}MB")
    
    def _get_cache_file_path(self, cache_type: str, fund_code: str, 
                           suffix: str = ".json") -> Path:
        """获取缓存文件路径"""
        if cache_type == "current":
            return self.current_data_dir / f"{fund_code}{suffix}"
        elif cache_type == "historical":
            return self.historical_data_dir / f"{fund_code}{suffix}"
        elif cache_type == "info":
            return self.fund_info_dir / f"{fund_code}{suffix}"
        else:
            raise ValueError(f"未知的缓存类型: {cache_type}")
    
    def _is_cache_valid(self, file_path: Path) -> bool:
        """检查缓存是否有效"""
        if not file_path.exists():
            return False
        
        # 检查文件修改时间
        file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        expire_time = datetime.now() - timedelta(hours=self.expire_hours)
        
        return file_time > expire_time
    
    def _save_json_cache(self, data: Dict[str, Any], file_path: Path, 
                        compress: bool = False) -> bool:
        """保存JSON缓存"""
        try:
            json_data = json.dumps(data, default=str, ensure_ascii=False, indent=2)
            
            if compress:
                # 压缩保存
                with gzip.open(f"{file_path}.gz", 'wt', encoding='utf-8') as f:
                    f.write(json_data)
            else:
                # 普通保存
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(json_data)
            
            return True
        except Exception as e:
            logger.error(f"保存JSON缓存失败: {file_path} - {e}")
            return False
    
    def _load_json_cache(self, file_path: Path, 
                        compressed: bool = False) -> Optional[Dict[str, Any]]:
        """加载JSON缓存"""
        try:
            actual_path = f"{file_path}.gz" if compressed else file_path
            
            if compressed and Path(actual_path).exists():
                with gzip.open(actual_path, 'rt', encoding='utf-8') as f:
                    return json.load(f)
            elif file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            return None
        except Exception as e:
            logger.error(f"加载JSON缓存失败: {file_path} - {e}")
            return None
    
    def _save_pickle_cache(self, data: Any, file_path: Path, 
                          compress: bool = True) -> bool:
        """保存Pickle缓存"""
        try:
            if compress:
                with gzip.open(f"{file_path}.pkl.gz", 'wb') as f:
                    pickle.dump(data, f)
            else:
                with open(f"{file_path}.pkl", 'wb') as f:
                    pickle.dump(data, f)
            
            return True
        except Exception as e:
            logger.error(f"保存Pickle缓存失败: {file_path} - {e}")
            return False
    
    def _load_pickle_cache(self, file_path: Path, 
                          compressed: bool = True) -> Optional[Any]:
        """加载Pickle缓存"""
        try:
            actual_path = f"{file_path}.pkl.gz" if compressed else f"{file_path}.pkl"
            
            if Path(actual_path).exists():
                if compressed:
                    with gzip.open(actual_path, 'rb') as f:
                        return pickle.load(f)
                else:
                    with open(actual_path, 'rb') as f:
                        return pickle.load(f)
            
            return None
        except Exception as e:
            logger.error(f"加载Pickle缓存失败: {file_path} - {e}")
            return None
    
    def cache_current_data(self, fund_code: str, fund_data: FundData) -> bool:
        """缓存当前数据"""
        file_path = self._get_cache_file_path("current", fund_code)
        
        cache_data = {
            'code': fund_data.code,
            'name': fund_data.name,
            'current_price': fund_data.current_price,
            'previous_close': fund_data.previous_close,
            'change_percent': fund_data.change_percent,
            'volume': fund_data.volume,
            'market_cap': fund_data.market_cap,
            'currency': fund_data.currency,
            'last_update': fund_data.last_update.isoformat(),
            'cache_time': datetime.now().isoformat()
        }
        
        success = self._save_json_cache(cache_data, file_path)
        if success:
            logger.debug(f"当前数据缓存成功: {fund_code}")
        
        return success
    
    def get_cached_current_data(self, fund_code: str) -> Optional[FundData]:
        """获取缓存的当前数据"""
        file_path = self._get_cache_file_path("current", fund_code)
        
        if not self._is_cache_valid(file_path):
            return None
        
        cache_data = self._load_json_cache(file_path)
        if not cache_data:
            return None
        
        try:
            fund_data = FundData(
                code=cache_data['code'],
                name=cache_data['name'],
                current_price=float(cache_data['current_price']),
                previous_close=float(cache_data['previous_close']),
                change_percent=float(cache_data['change_percent']),
                volume=int(cache_data['volume']),
                market_cap=cache_data.get('market_cap'),
                currency=cache_data.get('currency', 'USD'),
                last_update=datetime.fromisoformat(cache_data['last_update'])
            )
            
            logger.debug(f"当前数据缓存命中: {fund_code}")
            return fund_data
            
        except Exception as e:
            logger.error(f"解析缓存数据失败: {fund_code} - {e}")
            return None
    
    def cache_historical_data(self, fund_code: str, 
                            historical_data: HistoricalData) -> bool:
        """缓存历史数据"""
        file_path = self._get_cache_file_path("historical", fund_code, "")
        
        cache_data = {
            'code': historical_data.code,
            'start_date': historical_data.start_date.isoformat(),
            'end_date': historical_data.end_date.isoformat(),
            'period': historical_data.period,
            'cache_time': datetime.now().isoformat()
        }
        
        # 历史数据使用pickle保存DataFrame，更高效
        success = self._save_pickle_cache({
            'metadata': cache_data,
            'data': historical_data.data
        }, file_path)
        
        if success:
            logger.debug(f"历史数据缓存成功: {fund_code} ({historical_data.period})")
        
        return success
    
    def get_cached_historical_data(self, fund_code: str, 
                                 period: str = None) -> Optional[HistoricalData]:
        """获取缓存的历史数据"""
        file_path = self._get_cache_file_path("historical", fund_code, "")
        
        # 历史数据缓存时间更长，使用不同的过期策略
        if not file_path.with_suffix('.pkl.gz').exists():
            return None
        
        cache_data = self._load_pickle_cache(file_path)
        if not cache_data:
            return None
        
        try:
            metadata = cache_data['metadata']
            
            # 如果指定了period，检查是否匹配
            if period and metadata.get('period') != period:
                return None
            
            historical_data = HistoricalData(
                code=metadata['code'],
                data=cache_data['data'],
                start_date=datetime.fromisoformat(metadata['start_date']),
                end_date=datetime.fromisoformat(metadata['end_date']),
                period=metadata['period']
            )
            
            logger.debug(f"历史数据缓存命中: {fund_code} ({metadata['period']})")
            return historical_data
            
        except Exception as e:
            logger.error(f"解析历史数据缓存失败: {fund_code} - {e}")
            return None
    
    def cache_fund_info(self, fund_code: str, fund_info: Dict[str, Any]) -> bool:
        """缓存基金信息"""
        file_path = self._get_cache_file_path("info", fund_code)
        
        cache_data = {
            **fund_info,
            'cache_time': datetime.now().isoformat()
        }
        
        success = self._save_json_cache(cache_data, file_path)
        if success:
            logger.debug(f"基金信息缓存成功: {fund_code}")
        
        return success
    
    def get_cached_fund_info(self, fund_code: str) -> Optional[Dict[str, Any]]:
        """获取缓存的基金信息"""
        file_path = self._get_cache_file_path("info", fund_code)
        
        if not self._is_cache_valid(file_path):
            return None
        
        cache_data = self._load_json_cache(file_path)
        if cache_data:
            logger.debug(f"基金信息缓存命中: {fund_code}")
        
        return cache_data
    
    def clear_expired_cache(self) -> Dict[str, int]:
        """清理过期缓存"""
        cleared_count = {'current': 0, 'historical': 0, 'info': 0}
        
        for cache_type, cache_dir in [
            ('current', self.current_data_dir),
            ('historical', self.historical_data_dir),
            ('info', self.fund_info_dir)
        ]:
            for file_path in cache_dir.glob("*"):
                if not self._is_cache_valid(file_path):
                    try:
                        file_path.unlink()
                        cleared_count[cache_type] += 1
                    except Exception as e:
                        logger.error(f"删除过期缓存失败: {file_path} - {e}")
        
        total_cleared = sum(cleared_count.values())
        if total_cleared > 0:
            logger.info(f"清理过期缓存完成: {cleared_count}")
        
        return cleared_count
    
    def get_cache_size(self) -> Dict[str, float]:
        """获取缓存大小(MB)"""
        sizes = {}
        
        for cache_type, cache_dir in [
            ('current', self.current_data_dir),
            ('historical', self.historical_data_dir),
            ('info', self.fund_info_dir)
        ]:
            total_size = 0
            for file_path in cache_dir.glob("*"):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
            
            sizes[cache_type] = total_size / (1024 * 1024)  # 转换为MB
        
        sizes['total'] = sum(sizes.values())
        return sizes
    
    def cleanup_cache(self, force: bool = False) -> bool:
        """清理缓存"""
        try:
            # 先清理过期缓存
            self.clear_expired_cache()
            
            # 检查缓存大小
            cache_sizes = self.get_cache_size()
            
            if force or cache_sizes['total'] > self.max_cache_size_mb:
                logger.info(f"缓存大小超限({cache_sizes['total']:.1f}MB > {self.max_cache_size_mb}MB)，开始清理")
                
                # 按修改时间排序，删除最旧的文件
                all_files = []
                for cache_dir in [self.current_data_dir, self.historical_data_dir, self.fund_info_dir]:
                    for file_path in cache_dir.glob("*"):
                        if file_path.is_file():
                            all_files.append((file_path.stat().st_mtime, file_path))
                
                all_files.sort()  # 按时间排序，最旧的在前
                
                # 删除最旧的文件直到大小合适
                deleted_count = 0
                for _, file_path in all_files:
                    if cache_sizes['total'] <= self.max_cache_size_mb * 0.8:  # 保留80%空间
                        break
                    
                    try:
                        file_size = file_path.stat().st_size / (1024 * 1024)
                        file_path.unlink()
                        cache_sizes['total'] -= file_size
                        deleted_count += 1
                    except Exception as e:
                        logger.error(f"删除缓存文件失败: {file_path} - {e}")
                
                logger.info(f"缓存清理完成: 删除{deleted_count}个文件")
            
            return True
            
        except Exception as e:
            logger.error(f"缓存清理失败: {e}")
            return False
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """获取缓存统计信息"""
        sizes = self.get_cache_size()
        
        # 统计文件数量
        file_counts = {}
        for cache_type, cache_dir in [
            ('current', self.current_data_dir),
            ('historical', self.historical_data_dir),
            ('info', self.fund_info_dir)
        ]:
            file_counts[cache_type] = len(list(cache_dir.glob("*")))
        
        file_counts['total'] = sum(file_counts.values())
        
        return {
            'sizes_mb': sizes,
            'file_counts': file_counts,
            'expire_hours': self.expire_hours,
            'max_size_mb': self.max_cache_size_mb,
            'cache_dir': str(self.cache_dir)
        }
