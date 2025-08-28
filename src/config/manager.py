"""
全球指数基金量化分析平台配置管理器

负责加载、验证和管理系统配置，包括：
- 全球指数基金列表配置和筛选条件
- 量化策略参数配置  
- 数据源和存储配置
- 定时任务和报告配置
- 系统运行参数配置
"""

import os
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from loguru import logger


@dataclass
class SystemConfig:
    """系统配置数据类"""
    name: str = "Sora 全球指数基金量化分析平台"
    version: str = "2.0.0"
    environment: str = "production"
    data_dir: str = "data"
    database_dir: str = "data/database"
    cache_dir: str = "data/cache"
    reports_dir: str = "data/reports"
    logs_dir: str = "data/logs"


@dataclass
class DataSourceConfig:
    """数据源配置数据类"""
    # AKShare配置（用于国内基金）
    akshare_enabled: bool = True
    akshare_timeout: int = 30
    akshare_max_retries: int = 3
    akshare_retry_delay: float = 1.0
    akshare_rate_limit_delay: float = 0.5
    
    # yfinance配置（用于海外基金/ETF）
    yfinance_enabled: bool = True
    yfinance_timeout: int = 30
    yfinance_max_retries: int = 3
    yfinance_retry_delay: float = 1.0
    
    # 数据更新配置
    force_update_interval_hours: int = 24
    cache_expire_hours: int = 6
    batch_size: int = 20
    
    # 数据质量检查
    min_volume: int = 1000000
    max_price_change: float = 0.15
    data_completeness: float = 0.95


@dataclass
class FundSelectionConfig:
    """基金筛选配置数据类"""
    max_funds: int = 200
    min_market_cap: int = 1000000000
    min_daily_volume: int = 1000000
    min_listing_days: int = 252
    
    # 地区分布配置
    region_allocation: Dict[str, float] = None
    
    def __post_init__(self):
        if self.region_allocation is None:
            self.region_allocation = {
                'us_market': 0.35,      # 美国市场35%
                'china_market': 0.25,   # 中国市场25%
                'developed_markets': 0.20,  # 发达市场20%
                'emerging_markets': 0.15,   # 新兴市场15%
                'bonds_commodities': 0.05   # 债券商品5%
            }


@dataclass
class TechnicalIndicatorsConfig:
    """技术指标配置数据类"""
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    bollinger_period: int = 20
    bollinger_std: float = 2.0


@dataclass
class StrategyConfig:
    """策略配置数据类"""
    # 分析时间窗口
    short_term_days: int = 7
    medium_term_days: int = 21
    long_term_days: int = 63
    
    # 技术指标参数
    technical_indicators: TechnicalIndicatorsConfig = None
    
    # 决策阈值
    strong_buy_threshold: float = 0.8
    buy_threshold: float = 0.6
    hold_threshold: float = 0.4
    sell_threshold: float = 0.6
    strong_sell_threshold: float = 0.8
    
    # 风险管理
    max_single_position: float = 0.1
    stop_loss: float = 0.08
    take_profit: float = 0.15
    max_drawdown: float = 0.05
    
    def __post_init__(self):
        if self.technical_indicators is None:
            self.technical_indicators = TechnicalIndicatorsConfig()


@dataclass
class SchedulerConfig:
    """定时任务配置数据类"""
    timezone: str = "Asia/Shanghai"
    
    # 数据更新任务
    data_update_enabled: bool = True
    data_update_schedule: str = "0 9 * * 1"  # 每周一9点
    data_update_interval: int = 24  # 24小时间隔
    
    # 分析任务
    analysis_enabled: bool = True
    analysis_schedule: str = "30 9 * * 1"  # 每周一9点30分
    analysis_interval: int = 24  # 24小时间隔
    
    # 报告生成
    report_enabled: bool = True
    report_schedule: str = "0 10 * * 1"  # 每周一10点
    report_interval: int = 168  # 168小时（7天）间隔
    
    # 数据清理
    cleanup_enabled: bool = True
    cleanup_schedule: str = "0 2 * * 0"  # 每周日2点
    cleanup_retention_days: int = 90


@dataclass
class StorageConfig:
    """存储配置数据类"""
    # SQLite数据库
    database_file: str = "sora_funds.db"
    database_echo: bool = False
    pool_size: int = 5
    max_overflow: int = 10
    
    # 数据导出
    export_formats: List[str] = None
    excel_sheets: List[str] = None
    
    def __post_init__(self):
        if self.export_formats is None:
            self.export_formats = ["excel", "csv", "json"]
        if self.excel_sheets is None:
            self.excel_sheets = [
                "investment_signals",
                "performance_metrics", 
                "risk_analysis",
                "portfolio_allocation"
            ]


@dataclass
class LoggingConfig:
    """日志配置数据类"""
    level: str = "INFO"
    format: str = "detailed"
    
    # 文件日志
    file_enabled: bool = True
    max_size_mb: int = 10
    backup_count: int = 5
    
    # 控制台日志
    console_enabled: bool = True
    console_colored: bool = True


@dataclass
class ReportingConfig:
    """报告配置数据类"""
    # 投资建议报告
    signals_enabled: bool = True
    include_charts: bool = False
    confidence_threshold: float = 0.5
    
    # 绩效分析报告
    performance_enabled: bool = True
    benchmark: str = "SPY"  # 标普500作为基准
    
    # 风险分析报告
    risk_analysis_enabled: bool = True
    var_confidence: float = 0.95
    
    # 邮件通知
    email_enabled: bool = False
    smtp_server: str = ""
    smtp_port: int = 587
    email_username: str = ""
    email_password: str = ""
    email_recipients: List[str] = None
    
    def __post_init__(self):
        if self.email_recipients is None:
            self.email_recipients = []


@dataclass
class FundConfig:
    """基金配置数据类"""
    code: str
    name: str
    region: str = ""        # 地区：us, china, developed, emerging, etc.
    category: str = ""      # 分类：index_fund, etf, bond_fund, etc.
    underlying_index: str = ""  # 标的指数：sp500, nasdaq, csi300, etc.
    currency: str = "USD"   # 基准货币
    data_source: str = "yfinance"  # 数据源：yfinance, akshare
    priority: str = "medium"  # 优先级：high, medium, low
    enabled: bool = True
    reason: Optional[str] = None


class ConfigManager:
    """全球指数基金量化分析平台配置管理器"""
    
    def __init__(self, config_dir: str = "config"):
        """
        初始化配置管理器
        
        Args:
            config_dir: 配置文件目录路径
        """
        self.config_dir = Path(config_dir)
        self.settings_file = self.config_dir / "settings.yaml"
        self.funds_file = self.config_dir / "funds.yaml"
        self.fund_indexes_file = self.config_dir / "fund_indexes.yaml"  # 新增基金指数配置文件
        
        # 配置数据
        self._settings: Dict[str, Any] = {}
        self._funds_data: Dict[str, Any] = {}
        self._fund_indexes_data: Dict[str, Any] = {}
        
        # 配置对象
        self.system: Optional[SystemConfig] = None
        self.data_source: Optional[DataSourceConfig] = None
        self.fund_selection: Optional[FundSelectionConfig] = None
        self.strategy: Optional[StrategyConfig] = None
        self.scheduler: Optional[SchedulerConfig] = None
        self.storage: Optional[StorageConfig] = None
        self.logging: Optional[LoggingConfig] = None
        self.reporting: Optional[ReportingConfig] = None
        
        # 基金列表
        self.priority_funds: List[FundConfig] = []
        self.region_funds: Dict[str, List[FundConfig]] = {}
        self.all_funds: List[FundConfig] = []
        
        # 基金指数数据
        self.fund_indexes: Dict[str, Dict[str, Any]] = {}
        
        # 加载配置
        self.load_all_configs()
    
    def load_all_configs(self) -> None:
        """加载所有配置文件"""
        try:
            self.load_settings()
            self.load_funds()
            self.load_fund_indexes()
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
    
    def load_fund_indexes(self) -> None:
        """加载基金指数配置"""
        if not self.fund_indexes_file.exists():
            logger.warning(f"基金指数配置文件不存在: {self.fund_indexes_file}")
            self._create_default_fund_indexes()
        
        try:
            with open(self.fund_indexes_file, 'r', encoding='utf-8') as f:
                self._fund_indexes_data = yaml.safe_load(f) or {}
            
            # 解析基金指数配置
            self._parse_fund_indexes()
            logger.info(f"基金指数配置加载成功: {self.fund_indexes_file}")
            
        except Exception as e:
            logger.error(f"加载基金指数配置失败: {e}")
            raise

    def _parse_fund_indexes(self) -> None:
        """解析基金指数配置"""
        self.fund_indexes = self._fund_indexes_data.copy()
    
    def get_fund_index_info(self, index_name: str) -> Optional[Dict[str, Any]]:
        """获取基金指数信息"""
        return self.fund_indexes.get(index_name)
    
    def _create_default_settings(self) -> None:
        """创建默认设置配置文件"""
        default_settings = {
            'system': {
                'name': 'Sora 全球指数基金量化分析平台',
                'version': '2.0.0',
                'environment': 'production',
                'data_dir': 'data',
                'database_dir': 'data/database',
                'cache_dir': 'data/cache',
                'reports_dir': 'data/reports',
                'logs_dir': 'data/logs'
            },
            'data_source': {
                'akshare': {
                    'enabled': True,
                    'timeout': 30,
                    'max_retries': 3,
                    'retry_delay': 1.0,
                    'rate_limit_delay': 0.5
                },
                'yfinance': {
                    'enabled': True,
                    'timeout': 30,
                    'max_retries': 3,
                    'retry_delay': 1.0
                },
                'update': {
                    'force_update_interval_hours': 24,
                    'cache_expire_hours': 6,
                    'batch_size': 20
                },
                'quality_check': {
                    'min_volume': 1000000,
                    'max_price_change': 0.15,
                    'data_completeness': 0.95
                }
            },
            'fund_selection': {
                'max_funds': 200,
                'criteria': {
                    'min_market_cap': 1000000000,
                    'min_daily_volume': 1000000,
                    'min_listing_days': 252
                },
                'region_allocation': {
                    'us_market': 0.35,
                    'china_market': 0.25,
                    'developed_markets': 0.20,
                    'emerging_markets': 0.15,
                    'bonds_commodities': 0.05
                }
            },
            'strategy': {
                'analysis_windows': {
                    'short_term': 7,
                    'medium_term': 21,
                    'long_term': 63
                },
                'technical_indicators': {
                    'rsi_period': 14,
                    'macd_fast': 12,
                    'macd_slow': 26,
                    'macd_signal': 9,
                    'bollinger_period': 20,
                    'bollinger_std': 2.0
                },
                'thresholds': {
                    'strong_buy': 0.8,
                    'buy': 0.6,
                    'hold': 0.4,
                    'sell': 0.2,
                    'strong_sell': 0.1
                },
                'risk_management': {
                    'max_single_position': 0.1,
                    'stop_loss': 0.08,
                    'take_profit': 0.15,
                    'max_drawdown': 0.05
                }
            },
            'scheduler': {
                'data_update': {
                    'enabled': True,
                    'schedule': '0 9 * * 1',
                    'timezone': 'Asia/Shanghai'
                },
                'analysis': {
                    'enabled': True,
                    'schedule': '30 9 * * 1'
                },
                'report_generation': {
                    'enabled': True,
                    'schedule': '0 10 * * 1'
                },
                'data_cleanup': {
                    'enabled': True,
                    'schedule': '0 2 * * 0',
                    'retention_days': 90
                }
            },
            'storage': {
                'database': {
                    'file': 'sora_funds.db',
                    'echo': False,
                    'pool_size': 5,
                    'max_overflow': 10
                },
                'export': {
                    'formats': ['excel', 'csv', 'json'],
                    'excel_sheets': [
                        'investment_signals',
                        'performance_metrics',
                        'risk_analysis',
                        'portfolio_allocation'
                    ]
                }
            },
            'logging': {
                'level': 'INFO',
                'format': 'detailed',
                'file': {
                    'enabled': True,
                    'max_size_mb': 10,
                    'backup_count': 5
                },
                'console': {
                    'enabled': True,
                    'colored': True
                }
            },
            'reporting': {
                'investment_signals': {
                    'enabled': True,
                    'include_charts': False,
                    'confidence_threshold': 0.5
                },
                'performance': {
                    'enabled': True,
                    'benchmark': 'SPY'
                },
                'risk_analysis': {
                    'enabled': True,
                    'var_confidence': 0.95
                },
                'notifications': {
                    'email': {
                        'enabled': False,
                        'smtp_server': '',
                        'smtp_port': 587,
                        'username': '',
                        'password': '',
                        'recipients': []
                    }
                }
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
            'priority_funds': [
                {
                    'code': 'SPY',
                    'name': '标普500ETF',
                    'region': 'us_market',
                    'category': 'index_etf',
                    'underlying_index': 'sp500',
                    'currency': 'USD',
                    'data_source': 'yfinance',
                    'priority': 'high',
                    'enabled': True,
                    'reason': '美国大盘股代表，市场流动性最好'
                },
                {
                    'code': 'QQQ',
                    'name': '纳斯达克100ETF',
                    'region': 'us_market',
                    'category': 'index_etf',
                    'underlying_index': 'nasdaq100',
                    'currency': 'USD',
                    'data_source': 'yfinance',
                    'priority': 'high',
                    'enabled': True,
                    'reason': '美国科技股集中度高，成长性强'
                },
                {
                    'code': '159915',
                    'name': '创业板ETF',
                    'region': 'china_market',
                    'category': 'index_etf',
                    'underlying_index': 'chinext',
                    'currency': 'CNY',
                    'data_source': 'akshare',
                    'priority': 'high',
                    'enabled': True,
                    'reason': '中国成长股代表，科技含量高'
                }
            ],
            'region_funds': {
                'us_market': [
                    {
                        'code': 'VTI',
                        'name': '美国全市场ETF',
                        'category': 'index_etf',
                        'underlying_index': 'us_total_market',
                        'currency': 'USD',
                        'data_source': 'yfinance',
                        'priority': 'medium',
                        'enabled': True
                    },
                    {
                        'code': 'TLT',
                        'name': '美国长期国债ETF',
                        'category': 'bond_etf',
                        'underlying_index': 'us_treasury_20y',
                        'currency': 'USD',
                        'data_source': 'yfinance',
                        'priority': 'low',
                        'enabled': True
                    }
                ],
                'china_market': [
                    {
                        'code': '510300',
                        'name': '沪深300ETF',
                        'category': 'index_etf',
                        'underlying_index': 'csi300',
                        'currency': 'CNY',
                        'data_source': 'akshare',
                        'priority': 'high',
                        'enabled': True
                    },
                    {
                        'code': '510500',
                        'name': '中证500ETF',
                        'category': 'index_etf',
                        'underlying_index': 'csi500',
                        'currency': 'CNY',
                        'data_source': 'akshare',
                        'priority': 'medium',
                        'enabled': True
                    }
                ],
                'developed_markets': [
                    {
                        'code': 'EFA',
                        'name': '欧澳远东ETF',
                        'category': 'index_etf',
                        'underlying_index': 'eafe',
                        'currency': 'USD',
                        'data_source': 'yfinance',
                        'priority': 'medium',
                        'enabled': True
                    },
                    {
                        'code': 'EWJ',
                        'name': '日本ETF',
                        'category': 'country_etf',
                        'underlying_index': 'japan_msci',
                        'currency': 'USD',
                        'data_source': 'yfinance',
                        'priority': 'low',
                        'enabled': True
                    }
                ],
                'emerging_markets': [
                    {
                        'code': 'EEM',
                        'name': '新兴市场ETF',
                        'category': 'index_etf',
                        'underlying_index': 'emerging_markets',
                        'currency': 'USD',
                        'data_source': 'yfinance',
                        'priority': 'medium',
                        'enabled': True
                    },
                    {
                        'code': 'INDA',
                        'name': '印度ETF',
                        'category': 'country_etf',
                        'underlying_index': 'india_msci',
                        'currency': 'USD',
                        'data_source': 'yfinance',
                        'priority': 'low',
                        'enabled': True
                    },
                    {
                        'code': 'VanEck Vietnam ETF',
                        'name': '越南ETF',
                        'category': 'country_etf',
                        'underlying_index': 'vietnam_index',
                        'currency': 'USD',
                        'data_source': 'yfinance',
                        'priority': 'low',
                        'enabled': False,
                        'reason': '高风险新兴市场，需要谨慎'
                    }
                ],
                'bonds_commodities': [
                    {
                        'code': 'AGG',
                        'name': '美国总债券ETF',
                        'category': 'bond_etf',
                        'underlying_index': 'us_aggregate_bond',
                        'currency': 'USD',
                        'data_source': 'yfinance',
                        'priority': 'medium',
                        'enabled': True
                    },
                    {
                        'code': 'GLD',
                        'name': '黄金ETF',
                        'category': 'commodity_etf',
                        'underlying_index': 'gold_spot',
                        'currency': 'USD',
                        'data_source': 'yfinance',
                        'priority': 'low',
                        'enabled': True
                    }
                ]
            },
            'monitoring_config': {
                'auto_discovery': {
                    'enabled': True,
                    'frequency': 'monthly',
                    'criteria': {
                        'min_aum': 1000000000,  # 10亿资产
                        'min_volume': 5000000,   # 500万日交易量
                        'expense_ratio_max': 0.01  # 费率低于1%
                    }
                },
                'rebalancing': {
                    'enabled': True,
                    'frequency': 'quarterly',
                    'max_deviation': 0.05
                }
            }
        }
        
        # 确保目录存在
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        with open(self.funds_file, 'w', encoding='utf-8') as f:
            yaml.dump(default_funds, f, default_flow_style=False, 
                     allow_unicode=True, indent=2)
        
        logger.info(f"创建默认基金配置文件: {self.funds_file}")
    
    def _create_default_fund_indexes(self) -> None:
        """创建默认基金指数配置文件"""
        default_fund_indexes = {
            'us_indexes': {
                'sp500': {
                    'name': '标准普尔500指数',
                    'description': '美国500家大型公司股票指数',
                    'benchmark_symbol': '^GSPC',
                    'currency': 'USD',
                    'region': 'US',
                    'category': 'large_cap',
                    'popular_etfs': ['SPY', 'VOO', 'IVV'],
                    'popular_funds': [],
                    'key_metrics': {
                        'historical_return_10y': 0.13,
                        'volatility': 0.16,
                        'expense_ratio_typical': 0.0009
                    }
                },
                'nasdaq100': {
                    'name': '纳斯达克100指数',
                    'description': '纳斯达克100家最大非金融公司',
                    'benchmark_symbol': '^NDX',
                    'currency': 'USD',
                    'region': 'US',
                    'category': 'technology_growth',
                    'popular_etfs': ['QQQ', 'TQQQ'],
                    'popular_funds': [],
                    'key_metrics': {
                        'historical_return_10y': 0.17,
                        'volatility': 0.20,
                        'expense_ratio_typical': 0.002
                    }
                },
                'us_total_market': {
                    'name': '美国全市场指数',
                    'description': '覆盖美国全部股票市场',
                    'benchmark_symbol': 'VTI',
                    'currency': 'USD',
                    'region': 'US',
                    'category': 'total_market',
                    'popular_etfs': ['VTI', 'ITOT'],
                    'popular_funds': ['VTSAX'],
                    'key_metrics': {
                        'historical_return_10y': 0.12,
                        'volatility': 0.15,
                        'expense_ratio_typical': 0.0003
                    }
                }
            },
            'china_indexes': {
                'csi300': {
                    'name': '沪深300指数',
                    'description': '沪深两市300只大盘股',
                    'benchmark_symbol': '000300.SH',
                    'currency': 'CNY',
                    'region': 'China',
                    'category': 'large_cap',
                    'popular_etfs': ['510300', '159919'],
                    'popular_funds': [],
                    'key_metrics': {
                        'historical_return_5y': 0.07,
                        'volatility': 0.25,
                        'expense_ratio_typical': 0.005
                    }
                },
                'chinext': {
                    'name': '创业板指数',
                    'description': '深圳创业板综合指数',
                    'benchmark_symbol': '399006.SZ',
                    'currency': 'CNY',
                    'region': 'China',
                    'category': 'growth',
                    'popular_etfs': ['159915', '159952'],
                    'popular_funds': [],
                    'key_metrics': {
                        'historical_return_5y': 0.12,
                        'volatility': 0.30,
                        'expense_ratio_typical': 0.005
                    }
                },
                'csi500': {
                    'name': '中证500指数',
                    'description': '中证500只中小盘股票',
                    'benchmark_symbol': '000905.SH',
                    'currency': 'CNY',
                    'region': 'China',
                    'category': 'mid_cap',
                    'popular_etfs': ['510500', '159922'],
                    'popular_funds': [],
                    'key_metrics': {
                        'historical_return_5y': 0.05,
                        'volatility': 0.28,
                        'expense_ratio_typical': 0.005
                    }
                }
            },
            'international_indexes': {
                'eafe': {
                    'name': '欧澳远东指数',
                    'description': '发达市场(除美国外)',
                    'benchmark_symbol': 'EFA',
                    'currency': 'USD',
                    'region': 'International',
                    'category': 'developed_international',
                    'popular_etfs': ['EFA', 'VEA'],
                    'popular_funds': ['VTIAX'],
                    'key_metrics': {
                        'historical_return_10y': 0.07,
                        'volatility': 0.18,
                        'expense_ratio_typical': 0.0007
                    }
                },
                'emerging_markets': {
                    'name': '新兴市场指数',
                    'description': '新兴市场股票综合指数',
                    'benchmark_symbol': 'EEM',
                    'currency': 'USD',
                    'region': 'Emerging',
                    'category': 'emerging_markets',
                    'popular_etfs': ['EEM', 'VWO'],
                    'popular_funds': ['VEMAX'],
                    'key_metrics': {
                        'historical_return_10y': 0.04,
                        'volatility': 0.22,
                        'expense_ratio_typical': 0.001
                    }
                },
                'india_msci': {
                    'name': 'MSCI印度指数',
                    'description': '印度大中型股票指数',
                    'benchmark_symbol': 'INDA',
                    'currency': 'USD',
                    'region': 'India',
                    'category': 'single_country',
                    'popular_etfs': ['INDA', 'MINDX'],
                    'popular_funds': [],
                    'key_metrics': {
                        'historical_return_5y': 0.08,
                        'volatility': 0.25,
                        'expense_ratio_typical': 0.008
                    }
                },
                'vietnam_index': {
                    'name': '越南股票指数',
                    'description': '越南主要股票市场指数',
                    'benchmark_symbol': 'VNM',
                    'currency': 'USD',
                    'region': 'Vietnam',
                    'category': 'frontier_market',
                    'popular_etfs': ['VanEck Vietnam ETF'],
                    'popular_funds': [],
                    'key_metrics': {
                        'historical_return_5y': 0.06,
                        'volatility': 0.35,
                        'expense_ratio_typical': 0.015
                    }
                }
            },
            'bond_indexes': {
                'us_aggregate_bond': {
                    'name': '美国总债券指数',
                    'description': '美国投资级债券市场',
                    'benchmark_symbol': 'AGG',
                    'currency': 'USD',
                    'region': 'US',
                    'category': 'investment_grade_bond',
                    'popular_etfs': ['AGG', 'BND'],
                    'popular_funds': ['VBTLX'],
                    'key_metrics': {
                        'historical_return_10y': 0.03,
                        'volatility': 0.04,
                        'expense_ratio_typical': 0.0005
                    }
                },
                'us_treasury_20y': {
                    'name': '美国20年期国债',
                    'description': '20年以上美国国债',
                    'benchmark_symbol': 'TLT',
                    'currency': 'USD',
                    'region': 'US',
                    'category': 'long_term_treasury',
                    'popular_etfs': ['TLT', 'EDV'],
                    'popular_funds': [],
                    'key_metrics': {
                        'historical_return_10y': 0.04,
                        'volatility': 0.12,
                        'expense_ratio_typical': 0.0015
                    }
                }
            },
            'commodity_indexes': {
                'gold_spot': {
                    'name': '黄金现货',
                    'description': '黄金现货价格指数',
                    'benchmark_symbol': 'GLD',
                    'currency': 'USD',
                    'region': 'Global',
                    'category': 'precious_metal',
                    'popular_etfs': ['GLD', 'IAU'],
                    'popular_funds': [],
                    'key_metrics': {
                        'historical_return_10y': 0.01,
                        'volatility': 0.16,
                        'expense_ratio_typical': 0.004
                    }
                }
            }
        }
        
        # 确保目录存在
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        with open(self.fund_indexes_file, 'w', encoding='utf-8') as f:
            yaml.dump(default_fund_indexes, f, default_flow_style=False, 
                     allow_unicode=True, indent=2)
        
        logger.info(f"创建默认基金指数配置文件: {self.fund_indexes_file}")
    
    def _parse_settings(self) -> None:
        """解析系统设置配置"""
        # 系统配置
        system_config = self._settings.get('system', {})
        self.system = SystemConfig(
            name=system_config.get('name', 'Sora 全球指数基金量化分析平台'),
            version=system_config.get('version', '2.0.0'),
            environment=system_config.get('environment', 'production'),
            data_dir=system_config.get('data_dir', 'data'),
            database_dir=system_config.get('database_dir', 'data/database'),
            cache_dir=system_config.get('cache_dir', 'data/cache'),
            reports_dir=system_config.get('reports_dir', 'data/reports'),
            logs_dir=system_config.get('logs_dir', 'data/logs')
        )
        
        # 数据源配置
        data_source_config = self._settings.get('data_source', {})
        akshare_config = data_source_config.get('akshare', {})
        yfinance_config = data_source_config.get('yfinance', {})
        update_config = data_source_config.get('update', {})
        quality_config = data_source_config.get('quality_check', {})
        
        self.data_source = DataSourceConfig(
            akshare_enabled=akshare_config.get('enabled', True),
            akshare_timeout=akshare_config.get('timeout', 30),
            akshare_max_retries=akshare_config.get('max_retries', 3),
            akshare_retry_delay=akshare_config.get('retry_delay', 1.0),
            akshare_rate_limit_delay=akshare_config.get('rate_limit_delay', 0.5),
            yfinance_enabled=yfinance_config.get('enabled', True),
            yfinance_timeout=yfinance_config.get('timeout', 30),
            yfinance_max_retries=yfinance_config.get('max_retries', 3),
            yfinance_retry_delay=yfinance_config.get('retry_delay', 1.0),
            force_update_interval_hours=update_config.get('force_update_interval_hours', 24),
            cache_expire_hours=update_config.get('cache_expire_hours', 6),
            batch_size=update_config.get('batch_size', 20),
            min_volume=quality_config.get('min_volume', 1000000),
            max_price_change=quality_config.get('max_price_change', 0.15),
            data_completeness=quality_config.get('data_completeness', 0.95)
        )
        
        # 基金筛选配置
        fund_selection_config = self._settings.get('fund_selection', {})
        criteria_config = fund_selection_config.get('criteria', {})
        region_allocation = fund_selection_config.get('region_allocation', {})
        
        self.fund_selection = FundSelectionConfig(
            max_funds=fund_selection_config.get('max_funds', 200),
            min_market_cap=criteria_config.get('min_market_cap', 1000000000),
            min_daily_volume=criteria_config.get('min_daily_volume', 1000000),
            min_listing_days=criteria_config.get('min_listing_days', 252),
            region_allocation=region_allocation
        )
        
        # 策略配置
        strategy_config = self._settings.get('strategy', {})
        analysis_windows = strategy_config.get('analysis_windows', {})
        technical_indicators = strategy_config.get('technical_indicators', {})
        thresholds = strategy_config.get('thresholds', {})
        risk_management = strategy_config.get('risk_management', {})
        
        tech_indicators = TechnicalIndicatorsConfig(
            rsi_period=technical_indicators.get('rsi_period', 14),
            macd_fast=technical_indicators.get('macd_fast', 12),
            macd_slow=technical_indicators.get('macd_slow', 26),
            macd_signal=technical_indicators.get('macd_signal', 9),
            bollinger_period=technical_indicators.get('bollinger_period', 20),
            bollinger_std=technical_indicators.get('bollinger_std', 2.0)
        )
        
        self.strategy = StrategyConfig(
            short_term_days=analysis_windows.get('short_term', 7),
            medium_term_days=analysis_windows.get('medium_term', 21),
            long_term_days=analysis_windows.get('long_term', 63),
            technical_indicators=tech_indicators,
            strong_buy_threshold=thresholds.get('strong_buy', 0.8),
            buy_threshold=thresholds.get('buy', 0.6),
            hold_threshold=thresholds.get('hold', 0.4),
            sell_threshold=thresholds.get('sell', 0.6),
            strong_sell_threshold=thresholds.get('strong_sell', 0.8),
            max_single_position=risk_management.get('max_single_position', 0.1),
            stop_loss=risk_management.get('stop_loss', 0.08),
            take_profit=risk_management.get('take_profit', 0.15),
            max_drawdown=risk_management.get('max_drawdown', 0.05)
        )
        
        # 调度器配置
        scheduler_config = self._settings.get('scheduler', {})
        data_update = scheduler_config.get('data_update', {})
        analysis = scheduler_config.get('analysis', {})
        report_generation = scheduler_config.get('report_generation', {})
        data_cleanup = scheduler_config.get('data_cleanup', {})
        
        self.scheduler = SchedulerConfig(
            timezone=data_update.get('timezone', 'Asia/Shanghai'),
            data_update_enabled=data_update.get('enabled', True),
            data_update_schedule=data_update.get('schedule', '0 9 * * 1'),
            analysis_enabled=analysis.get('enabled', True),
            analysis_schedule=analysis.get('schedule', '30 9 * * 1'),
            report_enabled=report_generation.get('enabled', True),
            report_schedule=report_generation.get('schedule', '0 10 * * 1'),
            cleanup_enabled=data_cleanup.get('enabled', True),
            cleanup_schedule=data_cleanup.get('schedule', '0 2 * * 0'),
            cleanup_retention_days=data_cleanup.get('retention_days', 90)
        )
        
        # 存储配置
        storage_config = self._settings.get('storage', {})
        database_config = storage_config.get('database', {})
        export_config = storage_config.get('export', {})
        
        self.storage = StorageConfig(
            database_file=database_config.get('file', 'sora_funds.db'),
            database_echo=database_config.get('echo', False),
            pool_size=database_config.get('pool_size', 5),
            max_overflow=database_config.get('max_overflow', 10),
            export_formats=export_config.get('formats', ['excel', 'csv', 'json']),
            excel_sheets=export_config.get('excel_sheets', [
                'investment_signals', 'performance_metrics', 'risk_analysis', 'portfolio_allocation'
            ])
        )
        
        # 日志配置
        logging_config = self._settings.get('logging', {})
        file_config = logging_config.get('file', {})
        console_config = logging_config.get('console', {})
        
        self.logging = LoggingConfig(
            level=logging_config.get('level', 'INFO'),
            format=logging_config.get('format', 'detailed'),
            file_enabled=file_config.get('enabled', True),
            max_size_mb=file_config.get('max_size_mb', 10),
            backup_count=file_config.get('backup_count', 5),
            console_enabled=console_config.get('enabled', True),
            console_colored=console_config.get('colored', True)
        )
        
        # 报告配置
        reporting_config = self._settings.get('reporting', {})
        signals_config = reporting_config.get('investment_signals', {})
        performance_config = reporting_config.get('performance', {})
        risk_config = reporting_config.get('risk_analysis', {})
        notifications_config = reporting_config.get('notifications', {})
        email_config = notifications_config.get('email', {})
        
        self.reporting = ReportingConfig(
            signals_enabled=signals_config.get('enabled', True),
            include_charts=signals_config.get('include_charts', False),
            confidence_threshold=signals_config.get('confidence_threshold', 0.5),
            performance_enabled=performance_config.get('enabled', True),
            benchmark=performance_config.get('benchmark', 'SPY'),
            risk_analysis_enabled=risk_config.get('enabled', True),
            var_confidence=risk_config.get('var_confidence', 0.95),
            email_enabled=email_config.get('enabled', False),
            smtp_server=email_config.get('smtp_server', ''),
            smtp_port=email_config.get('smtp_port', 587),
            email_username=email_config.get('username', ''),
            email_password=email_config.get('password', ''),
            email_recipients=email_config.get('recipients', [])
        )
    
    def _parse_funds(self) -> None:
        """解析基金配置"""
        # 解析优先级基金
        priority_funds_data = self._funds_data.get('priority_funds', [])
        self.priority_funds = []
        for fund_data in priority_funds_data:
            fund_config = FundConfig(
                code=fund_data['code'],
                name=fund_data['name'],
                region=fund_data.get('region', ''),
                category=fund_data.get('category', ''),
                underlying_index=fund_data.get('underlying_index', ''),
                currency=fund_data.get('currency', 'USD'),
                data_source=fund_data.get('data_source', 'yfinance'),
                priority=fund_data.get('priority', 'medium'),
                enabled=fund_data.get('enabled', True),
                reason=fund_data.get('reason')
            )
            self.priority_funds.append(fund_config)
        
        # 解析区域基金
        region_funds_data = self._funds_data.get('region_funds', {})
        self.region_funds = {}
        for region, funds_list in region_funds_data.items():
            self.region_funds[region] = []
            for fund_data in funds_list:
                fund_config = FundConfig(
                    code=fund_data['code'],
                    name=fund_data['name'],
                    region=region,
                    category=fund_data.get('category', ''),
                    underlying_index=fund_data.get('underlying_index', ''),
                    currency=fund_data.get('currency', 'USD'),
                    data_source=fund_data.get('data_source', 'yfinance'),
                    priority=fund_data.get('priority', 'medium'),
                    enabled=fund_data.get('enabled', True),
                    reason=fund_data.get('reason')
                )
                self.region_funds[region].append(fund_config)
        
        # 合并所有基金
        self.all_funds = self.priority_funds.copy()
        for region_funds in self.region_funds.values():
            self.all_funds.extend(region_funds)
    
    def get_enabled_funds(self) -> List[FundConfig]:
        """获取所有启用的基金"""
        return [fund for fund in self.all_funds if fund.enabled]
    
    def get_funds_by_region(self, region: str) -> List[FundConfig]:
        """根据地区获取基金列表"""
        # 从优先级基金中筛选
        priority_region_funds = [f for f in self.priority_funds if f.region == region and f.enabled]
        
        # 从区域基金中获取
        region_funds = self.region_funds.get(region, [])
        enabled_region_funds = [f for f in region_funds if f.enabled]
        
        # 合并并去重（优先级基金优先）
        all_region_funds = priority_region_funds + enabled_region_funds
        unique_codes = set()
        unique_funds = []
        for fund in all_region_funds:
            if fund.code not in unique_codes:
                unique_codes.add(fund.code)
                unique_funds.append(fund)
        
        return unique_funds
    
    def get_funds_by_priority(self, priority: str) -> List[FundConfig]:
        """根据优先级获取基金列表"""
        return [fund for fund in self.all_funds if fund.priority == priority and fund.enabled]
    
    def get_funds_by_data_source(self, data_source: str) -> List[FundConfig]:
        """根据数据源获取基金列表"""
        return [fund for fund in self.all_funds if fund.data_source == data_source and fund.enabled]
    
    def get_fund_by_code(self, code: str) -> Optional[FundConfig]:
        """根据代码获取基金配置"""
        for fund in self.all_funds:
            if fund.code == code:
                return fund
        return None
    
    def validate_config(self) -> Dict[str, List[str]]:
        """验证配置的完整性和正确性"""
        errors = {
            'settings': [],
            'funds': [],
            'fund_indexes': []
        }
        
        # 验证设置配置
        if not self.system:
            errors['settings'].append("系统配置缺失")
        if not self.data_source:
            errors['settings'].append("数据源配置缺失")
        if not self.strategy:
            errors['settings'].append("策略配置缺失")
        
        # 验证基金配置
        if not self.all_funds:
            errors['funds'].append("基金列表为空")
        
        # 验证基金代码格式
        for fund in self.all_funds:
            if not fund.code:
                errors['funds'].append(f"基金代码不能为空: {fund.name}")
            if fund.data_source == 'akshare' and not fund.code.isdigit():
                errors['funds'].append(f"中国基金代码格式错误: {fund.code}")
        
        # 验证区域分配
        if self.fund_selection and self.fund_selection.region_allocation:
            total_allocation = sum(self.fund_selection.region_allocation.values())
            if abs(total_allocation - 1.0) > 0.01:
                errors['funds'].append(f"区域分配总和不等于1: {total_allocation}")
        
        # 验证策略参数
        if self.strategy:
            if self.strategy.strong_buy_threshold <= self.strategy.buy_threshold:
                errors['settings'].append("强买入阈值应大于买入阈值")
            if self.strategy.sell_threshold <= self.strategy.strong_sell_threshold:
                errors['settings'].append("卖出阈值应大于强卖出阈值")
            if self.strategy.max_single_position > 0.5:
                errors['settings'].append("单个持仓比例不应超过50%")
        
        # 验证基金指数配置
        if not self.fund_indexes:
            errors['fund_indexes'].append("基金指数配置为空")
        
        return errors
    
    def get_config_summary(self) -> Dict[str, Any]:
        """获取配置摘要信息"""
        summary = {
            'system': {
                'name': self.system.name if self.system else 'N/A',
                'version': self.system.version if self.system else 'N/A',
                'environment': self.system.environment if self.system else 'N/A'
            },
            'funds': {
                'total_funds': len(self.all_funds),
                'enabled_funds': len(self.get_enabled_funds()),
                'priority_funds': len(self.priority_funds),
                'regions': list(self.region_funds.keys())
            },
            'data_sources': {
                'akshare_enabled': self.data_source.akshare_enabled if self.data_source else False,
                'yfinance_enabled': self.data_source.yfinance_enabled if self.data_source else False
            },
            'strategy': {
                'analysis_windows': {
                    'short_term': self.strategy.short_term_days if self.strategy else 'N/A',
                    'medium_term': self.strategy.medium_term_days if self.strategy else 'N/A',
                    'long_term': self.strategy.long_term_days if self.strategy else 'N/A'
                } if self.strategy else {}
            },
            'scheduler': {
                'data_update_enabled': self.scheduler.data_update_enabled if self.scheduler else False,
                'analysis_enabled': self.scheduler.analysis_enabled if self.scheduler else False,
                'report_enabled': self.scheduler.report_enabled if self.scheduler else False
            },
            'fund_indexes': {
                'total_indexes': len(self.fund_indexes),
                'index_categories': list(self.fund_indexes.keys())
            }
        }
        
        return summary
    
    def get_fund_statistics(self) -> Dict[str, Any]:
        """获取基金统计信息"""
        enabled_funds = self.get_enabled_funds()
        
        # 按地区统计
        region_stats = {}
        for region in self.region_funds.keys():
            region_funds = self.get_funds_by_region(region)
            region_stats[region] = {
                'count': len(region_funds),
                'funds': [f.code for f in region_funds]
            }
        
        # 按优先级统计
        priority_stats = {}
        for priority in ['high', 'medium', 'low']:
            priority_funds = self.get_funds_by_priority(priority)
            priority_stats[priority] = {
                'count': len(priority_funds),
                'funds': [f.code for f in priority_funds]
            }
        
        # 按数据源统计
        data_source_stats = {}
        for data_source in ['akshare', 'yfinance']:
            ds_funds = self.get_funds_by_data_source(data_source)
            data_source_stats[data_source] = {
                'count': len(ds_funds),
                'funds': [f.code for f in ds_funds]
            }
        
        # 按分类统计
        category_stats = {}
        for fund in enabled_funds:
            if fund.category not in category_stats:
                category_stats[fund.category] = []
            category_stats[fund.category].append(fund.code)
        
        return {
            'total_funds': len(self.all_funds),
            'enabled_funds': len(enabled_funds),
            'disabled_funds': len(self.all_funds) - len(enabled_funds),
            'by_region': region_stats,
            'by_priority': priority_stats,
            'by_data_source': data_source_stats,
            'by_category': {k: {'count': len(v), 'funds': v} for k, v in category_stats.items()}
        }
    
    def update_fund_status(self, fund_code: str, enabled: bool) -> bool:
        """更新基金启用状态"""
        fund = self.get_fund_by_code(fund_code)
        if fund:
            fund.enabled = enabled
            logger.info(f"基金 {fund_code} 状态更新为: {'启用' if enabled else '禁用'}")
            return True
        else:
            logger.warning(f"未找到基金: {fund_code}")
            return False
    
    def reload_config(self) -> None:
        """重新加载配置"""
        logger.info("重新加载配置文件...")
        self.load_all_configs()
        logger.info("配置文件重新加载完成")
    
    def save_current_config(self) -> None:
        """保存当前配置到文件"""
        # 这里可以实现将当前内存中的配置保存到YAML文件的逻辑
        # 暂时只记录日志
        logger.info("保存当前配置到文件（功能待实现）")
    
    def __str__(self) -> str:
        """返回配置管理器的字符串表示"""
        summary = self.get_config_summary()
        return (f"ConfigManager("
                f"funds={summary['funds']['total_funds']}, "
                f"enabled={summary['funds']['enabled_funds']}, "
                f"regions={len(summary['funds']['regions'])}, "
                f"indexes={summary['fund_indexes']['total_indexes']})")
    
    def __repr__(self) -> str:
        """返回配置管理器的详细表示"""
        return self.__str__()
