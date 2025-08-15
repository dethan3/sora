"""
配置管理器

负责加载、验证和管理系统配置，包括：
- 基金列表配置
- 决策参数配置  
- 系统运行参数配置
- 配置文件热更新
"""

import os
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from loguru import logger


@dataclass
class FundConfig:
    """基金配置数据类"""
    code: str
    name: str
    purchase_price: Optional[float] = None
    purchase_date: Optional[str] = None
    reason: Optional[str] = None


@dataclass
class StrategyConfig:
    """策略配置数据类"""
    buy_threshold: float = 0.95
    sell_threshold: float = 1.05
    analysis_days: int = 60
    max_confidence: float = 0.9


@dataclass
class DataConfig:
    """数据源配置数据类"""
    update_interval: int = 30
    cache_expire_hours: int = 24
    request_timeout: int = 10
    max_retries: int = 3


@dataclass
class OutputConfig:
    """输出配置数据类"""
    format: str = "table"
    verbose: bool = True
    colored: bool = True


@dataclass
class LoggingConfig:
    """日志配置数据类"""
    level: str = "INFO"
    file: str = "data/logs/fund_tracker.log"
    max_size: int = 10
    backup_count: int = 5


@dataclass
class SystemConfig:
    """系统配置数据类"""
    work_dir: str = "."
    data_dir: str = "data"
    cache_dir: str = "data/cache"


class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_dir: str = "config"):
        """
        初始化配置管理器
        
        Args:
            config_dir: 配置文件目录路径
        """
        self.config_dir = Path(config_dir)
        self.settings_file = self.config_dir / "settings.yaml"
        self.funds_file = self.config_dir / "funds.yaml"
        
        # 配置数据
        self._settings: Dict[str, Any] = {}
        self._funds_data: Dict[str, Any] = {}
        
        # 配置对象
        self.strategy: Optional[StrategyConfig] = None
        self.data: Optional[DataConfig] = None
        self.output: Optional[OutputConfig] = None
        self.logging: Optional[LoggingConfig] = None
        self.system: Optional[SystemConfig] = None
        
        # 基金列表
        self.owned_funds: List[FundConfig] = []
        self.watchlist_funds: List[FundConfig] = []
        self.fund_groups: Dict[str, List[str]] = {}
        
        # 加载配置
        self.load_all_configs()
    
    def load_all_configs(self) -> None:
        """加载所有配置文件"""
        try:
            self.load_settings()
            self.load_funds()
            logger.info("所有配置文件加载成功")
        except Exception as e:
            logger.error(f"配置文件加载失败: {e}")
            raise
    
    def load_settings(self) -> None:
        """加载系统设置配置"""
        if not self.settings_file.exists():
            logger.warning(f"设置配置文件不存在: {self.settings_file}")
            self._create_default_settings()
        
        try:
            with open(self.settings_file, 'r', encoding='utf-8') as f:
                self._settings = yaml.safe_load(f) or {}
            
            # 解析配置对象
            self._parse_settings()
            logger.info(f"系统设置配置加载成功: {self.settings_file}")
            
        except Exception as e:
            logger.error(f"加载系统设置配置失败: {e}")
            raise
    
    def load_funds(self) -> None:
        """加载基金配置"""
        if not self.funds_file.exists():
            logger.warning(f"基金配置文件不存在: {self.funds_file}")
            self._create_default_funds()
        
        try:
            with open(self.funds_file, 'r', encoding='utf-8') as f:
                self._funds_data = yaml.safe_load(f) or {}
            
            # 解析基金配置
            self._parse_funds()
            logger.info(f"基金配置加载成功: {self.funds_file}")
            
        except Exception as e:
            logger.error(f"加载基金配置失败: {e}")
            raise
    
    def _parse_settings(self) -> None:
        """解析系统设置配置"""
        # 策略配置
        strategy_data = self._settings.get('strategy', {})
        self.strategy = StrategyConfig(
            buy_threshold=strategy_data.get('buy_threshold', 0.95),
            sell_threshold=strategy_data.get('sell_threshold', 1.05),
            analysis_days=strategy_data.get('analysis_days', 60),
            max_confidence=strategy_data.get('max_confidence', 0.9)
        )
        
        # 数据源配置
        data_config = self._settings.get('data', {})
        self.data = DataConfig(
            update_interval=data_config.get('update_interval', 30),
            cache_expire_hours=data_config.get('cache_expire_hours', 24),
            request_timeout=data_config.get('request_timeout', 10),
            max_retries=data_config.get('max_retries', 3)
        )
        
        # 输出配置
        output_config = self._settings.get('output', {})
        self.output = OutputConfig(
            format=output_config.get('format', 'table'),
            verbose=output_config.get('verbose', True),
            colored=output_config.get('colored', True)
        )
        
        # 日志配置
        logging_config = self._settings.get('logging', {})
        self.logging = LoggingConfig(
            level=logging_config.get('level', 'INFO'),
            file=logging_config.get('file', 'data/logs/fund_tracker.log'),
            max_size=logging_config.get('max_size', 10),
            backup_count=logging_config.get('backup_count', 5)
        )
        
        # 系统配置
        system_config = self._settings.get('system', {})
        self.system = SystemConfig(
            work_dir=system_config.get('work_dir', '.'),
            data_dir=system_config.get('data_dir', 'data'),
            cache_dir=system_config.get('cache_dir', 'data/cache')
        )
    
    def _parse_funds(self) -> None:
        """解析基金配置"""
        # 持有基金
        owned_funds_data = self._funds_data.get('owned_funds', [])
        self.owned_funds = [
            FundConfig(
                code=fund.get('code', ''),
                name=fund.get('name', ''),
                purchase_price=fund.get('purchase_price'),
                purchase_date=fund.get('purchase_date'),
                reason=fund.get('reason')
            )
            for fund in owned_funds_data
        ]
        
        # 关注基金
        watchlist_funds_data = self._funds_data.get('watchlist_funds', [])
        self.watchlist_funds = [
            FundConfig(
                code=fund.get('code', ''),
                name=fund.get('name', ''),
                purchase_price=fund.get('purchase_price'),
                purchase_date=fund.get('purchase_date'),
                reason=fund.get('reason')
            )
            for fund in watchlist_funds_data
        ]
        
        # 基金分组
        self.fund_groups = self._funds_data.get('groups', {})
    
    def get_all_fund_codes(self) -> List[str]:
        """获取所有基金代码"""
        codes = []
        codes.extend([fund.code for fund in self.owned_funds])
        codes.extend([fund.code for fund in self.watchlist_funds])
        return list(set(codes))  # 去重
    
    def get_owned_fund_codes(self) -> List[str]:
        """获取持有基金代码"""
        return [fund.code for fund in self.owned_funds]
    
    def get_watchlist_fund_codes(self) -> List[str]:
        """获取关注基金代码"""
        return [fund.code for fund in self.watchlist_funds]
    
    def get_fund_by_code(self, code: str) -> Optional[FundConfig]:
        """根据代码获取基金配置"""
        # 先在持有基金中查找
        for fund in self.owned_funds:
            if fund.code == code:
                return fund
        
        # 再在关注基金中查找
        for fund in self.watchlist_funds:
            if fund.code == code:
                return fund
        
        return None
    
    def is_owned_fund(self, code: str) -> bool:
        """判断是否为持有基金"""
        return code in self.get_owned_fund_codes()
    
    def get_funds_by_group(self, group_name: str) -> List[str]:
        """根据分组获取基金代码列表"""
        return self.fund_groups.get(group_name, [])
    
    def validate_config(self) -> bool:
        """验证配置有效性"""
        try:
            # 验证策略参数
            if not (0 < self.strategy.buy_threshold < 1):
                logger.error(f"买入阈值无效: {self.strategy.buy_threshold}")
                return False
            
            if not (1 < self.strategy.sell_threshold < 2):
                logger.error(f"卖出阈值无效: {self.strategy.sell_threshold}")
                return False
            
            if self.strategy.buy_threshold >= self.strategy.sell_threshold:
                logger.error("买入阈值不能大于等于卖出阈值")
                return False
            
            # 验证基金代码
            all_codes = self.get_all_fund_codes()
            if not all_codes:
                logger.error("没有配置任何基金")
                return False
            
            # 验证基金代码格式
            for code in all_codes:
                if not code or not code.isdigit():
                    logger.error(f"基金代码格式无效: {code}")
                    return False
            
            logger.info("配置验证通过")
            return True
            
        except Exception as e:
            logger.error(f"配置验证失败: {e}")
            return False
    
    def _create_default_settings(self) -> None:
        """创建默认设置配置文件"""
        default_settings = {
            'strategy': {
                'buy_threshold': 0.95,
                'sell_threshold': 1.05,
                'analysis_days': 60,
                'max_confidence': 0.9
            },
            'data': {
                'update_interval': 30,
                'cache_expire_hours': 24,
                'request_timeout': 10,
                'max_retries': 3
            },
            'output': {
                'format': 'table',
                'verbose': True,
                'colored': True
            },
            'logging': {
                'level': 'INFO',
                'file': 'data/logs/fund_tracker.log',
                'max_size': 10,
                'backup_count': 5
            },
            'system': {
                'work_dir': '.',
                'data_dir': 'data',
                'cache_dir': 'data/cache'
            }
        }
        
        # 确保目录存在
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        with open(self.settings_file, 'w', encoding='utf-8') as f:
            yaml.dump(default_settings, f, default_flow_style=False, 
                     allow_unicode=True, indent=2)
        
        logger.info(f"创建默认设置配置文件: {self.settings_file}")
    
    def _create_default_funds(self) -> None:
        """创建默认基金配置文件"""
        default_funds = {
            'owned_funds': [
                {
                    'code': '110022',
                    'name': '易方达消费行业',
                    'purchase_price': 3.1500,
                    'purchase_date': '2024-01-15'
                }
            ],
            'watchlist_funds': [
                {
                    'code': '270042',
                    'name': '广发纳指联接A',
                    'reason': '美股指数基金，分散风险'
                }
            ],
            'groups': {
                'equity': ['110022'],
                'international': ['270042']
            }
        }
        
        # 确保目录存在
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        with open(self.funds_file, 'w', encoding='utf-8') as f:
            yaml.dump(default_funds, f, default_flow_style=False, 
                     allow_unicode=True, indent=2)
        
        logger.info(f"创建默认基金配置文件: {self.funds_file}")
    
    def reload_config(self) -> None:
        """重新加载配置"""
        logger.info("重新加载配置文件...")
        self.load_all_configs()
    
    def get_config_summary(self) -> Dict[str, Any]:
        """获取配置摘要"""
        return {
            'strategy': {
                'buy_threshold': self.strategy.buy_threshold,
                'sell_threshold': self.strategy.sell_threshold,
                'analysis_days': self.strategy.analysis_days
            },
            'funds': {
                'owned_count': len(self.owned_funds),
                'watchlist_count': len(self.watchlist_funds),
                'total_count': len(self.get_all_fund_codes())
            },
            'groups': list(self.fund_groups.keys())
        }
