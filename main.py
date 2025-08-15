#!/usr/bin/env python3
"""
Sora - AI 辅助投资决策的基金跟踪系统

主程序入口，提供命令行界面和核心功能调度。
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import click
from loguru import logger
from src.config import ConfigManager
from src.data import DataFetcher, DataCache
from src.analytics import AnalyticsCalculator
from src.decision import DecisionEngine


@click.command()
@click.option('--config-dir', '-c', default='config', 
              help='配置文件目录路径')
@click.option('--verbose', '-v', is_flag=True, 
              help='显示详细输出')
@click.option('--dry-run', '-d', is_flag=True,
              help='模拟运行，不执行实际操作')
def main(config_dir: str, verbose: bool, dry_run: bool):
    """
    🤖 Sora - AI 智能基金投资助手
    
    基于历史数据分析，为您提供智能的基金投资建议。
    """
    
    # 设置日志
    setup_logging(verbose)
    
    try:
        # 显示欢迎信息
        display_welcome()
        
        # 初始化配置管理器
        logger.info("正在初始化配置管理器...")
        config_manager = ConfigManager(config_dir)
        
        # 验证配置
        if not config_manager.validate_config():
            logger.error("配置验证失败，程序退出")
            sys.exit(1)
        
        # 显示配置摘要
        display_config_summary(config_manager)
        
        if dry_run:
            logger.info("模拟运行模式，程序退出")
            return
        
        # 初始化数据获取器和缓存
        logger.info("正在初始化数据获取模块...")
        data_fetcher = DataFetcher(
            request_timeout=config_manager.data.request_timeout,
            max_retries=config_manager.data.max_retries,
            batch_size=5,  # 限制批大小避免API限流
            rate_limit_delay=0.2
        )
        
        data_cache = DataCache(
            cache_dir=config_manager.system.cache_dir,
            expire_hours=config_manager.data.cache_expire_hours
        )
        
        # 初始化分析和决策模块
        logger.info("正在初始化分析和决策模块...")
        analytics_calculator = AnalyticsCalculator(
            analysis_days=config_manager.strategy.analysis_days
        )
        
        decision_engine = DecisionEngine(config_manager.strategy)
        
        # 运行完整的分析流程
        run_full_analysis(config_manager, data_fetcher, data_cache, 
                         analytics_calculator, decision_engine)
        
    except KeyboardInterrupt:
        logger.info("用户中断程序")
        sys.exit(0)
    except Exception as e:
        logger.error(f"程序运行出错: {e}")
        sys.exit(1)


def setup_logging(verbose: bool = False):
    """设置日志配置"""
    # 移除默认处理器
    logger.remove()
    
    # 设置日志级别
    level = "DEBUG" if verbose else "INFO"
    
    # 添加控制台输出
    logger.add(
        sys.stderr,
        level=level,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True
    )
    
    # 创建日志目录
    log_dir = Path("data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # 添加文件输出
    logger.add(
        "data/logs/sora.log",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        rotation="10 MB",
        retention="7 days",
        compression="zip"
    )


def display_welcome():
    """显示欢迎信息"""
    welcome_text = """
🤖 ═══════════════════════════════════════════════════════════════
    Sora - AI 智能基金投资助手 v0.1.0
    让 AI 成为您的专属投资顾问，告别盲目投资！
═══════════════════════════════════════════════════════════════
"""
    print(welcome_text)


def display_config_summary(config_manager: ConfigManager):
    """显示配置摘要"""
    summary = config_manager.get_config_summary()
    
    print("\n📋 配置摘要:")
    print("─" * 50)
    print(f"📈 投资策略:")
    print(f"   买入阈值: {summary['strategy']['buy_threshold']:.1%}")
    print(f"   卖出阈值: {summary['strategy']['sell_threshold']:.1%}")
    print(f"   分析天数: {summary['strategy']['analysis_days']} 天")
    
    print(f"\n💼 基金配置:")
    print(f"   持有基金: {summary['funds']['owned_count']} 只")
    print(f"   关注基金: {summary['funds']['watchlist_count']} 只")
    print(f"   总计基金: {summary['funds']['total_count']} 只")
    
    if summary['groups']:
        print(f"   基金分组: {', '.join(summary['groups'])}")
    
    print("─" * 50)
    
    # 显示基金列表
    print("\n📊 持有基金列表:")
    for fund in config_manager.owned_funds:
        price_info = f" (买入价: ¥{fund.purchase_price})" if fund.purchase_price else ""
        print(f"   🟢 {fund.code} - {fund.name}{price_info}")
    
    if config_manager.watchlist_funds:
        print("\n👀 关注基金列表:")
        for fund in config_manager.watchlist_funds:
            reason_info = f" - {fund.reason}" if fund.reason else ""
            print(f"   🟡 {fund.code} - {fund.name}{reason_info}")
    
    print()


def test_data_fetching(config_manager: ConfigManager, data_fetcher: DataFetcher, 
                      data_cache: DataCache):
    """测试数据获取功能"""
    logger.info("开始测试数据获取功能...")
    
    # 获取测试基金代码（取前3只避免API限流）
    test_codes = config_manager.get_all_fund_codes()[:3]
    logger.info(f"测试基金代码: {test_codes}")
    
    print("\n🔍 数据获取测试:")
    print("─" * 50)
    
    # 测试单个基金数据获取
    if test_codes:
        test_code = test_codes[0]
        print(f"\n📊 测试单个基金数据获取: {test_code}")
        
        # 检查缓存
        cached_data = data_cache.get_cached_current_data(test_code)
        if cached_data:
            print(f"   ✅ 缓存命中: {cached_data.name} - ¥{cached_data.current_price:.4f}")
        else:
            print(f"   🔄 从API获取数据...")
            
            # 获取当前数据
            current_data = data_fetcher.get_current_data(test_code)
            if current_data:
                print(f"   ✅ 获取成功: {current_data.name}")
                print(f"      当前价格: ¥{current_data.current_price:.4f}")
                print(f"      涨跌幅: {current_data.change_percent:+.2f}%")
                print(f"      更新时间: {current_data.last_update.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # 缓存数据
                data_cache.cache_current_data(test_code, current_data)
                print(f"   💾 数据已缓存")
            else:
                print(f"   ❌ 获取失败")
    
    # 测试批量数据获取（仅在有多个基金时）
    if len(test_codes) > 1:
        print(f"\n📈 测试批量数据获取: {len(test_codes)} 只基金")
        
        batch_data = data_fetcher.batch_get_current_data(test_codes)
        success_count = len(batch_data)
        
        print(f"   📊 批量获取结果: {success_count}/{len(test_codes)} 成功")
        
        for code, fund_data in batch_data.items():
            fund_config = config_manager.get_fund_by_code(code)
            status = "持有" if config_manager.is_owned_fund(code) else "关注"
            
            print(f"   🟢 {code} - {fund_data.name} ({status})")
            print(f"      价格: ¥{fund_data.current_price:.4f} ({fund_data.change_percent:+.2f}%)")
            
            # 缓存数据
            data_cache.cache_current_data(code, fund_data)
    
    # 显示缓存统计
    cache_stats = data_cache.get_cache_stats()
    print(f"\n💾 缓存统计:")
    print(f"   文件数量: {cache_stats['file_counts']['total']} 个")
    print(f"   缓存大小: {cache_stats['sizes_mb']['total']:.2f} MB")
    print(f"   缓存目录: {cache_stats['cache_dir']}")
    
    print("─" * 50)
    logger.info("数据获取模块测试完成！")


def run_full_analysis(config_manager: ConfigManager, data_fetcher: DataFetcher,
                     data_cache: DataCache, analytics_calculator: AnalyticsCalculator,
                     decision_engine: DecisionEngine):
    """运行完整的分析流程"""
    logger.info("开始运行完整的基金分析流程...")
    
    # 获取所有基金代码
    all_fund_codes = config_manager.get_all_fund_codes()
    logger.info(f"分析基金总数: {len(all_fund_codes)}")
    
    print("\n🚀 Sora AI 基金分析报告")
    print("═" * 60)
    
    # 1. 数据获取阶段
    print("\n📊 第一阶段：数据获取")
    print("─" * 30)
    
    # 批量获取当前数据
    current_data = data_fetcher.batch_get_current_data(all_fund_codes)
    success_count = len(current_data)
    
    print(f"✅ 数据获取完成: {success_count}/{len(all_fund_codes)} 只基金")
    
    if not current_data:
        print("❌ 未能获取到任何基金数据，分析终止")
        return
    
    # 缓存数据
    for code, fund_data in current_data.items():
        data_cache.cache_current_data(code, fund_data)
    
    # 2. 分析计算阶段
    print(f"\n🔍 第二阶段：技术分析")
    print("─" * 30)
    
    # 批量分析
    analysis_results = analytics_calculator.batch_analyze(list(current_data.values()))
    
    print(f"✅ 技术分析完成: {len(analysis_results)} 只基金")
    
    # 显示分析摘要
    analysis_summary = analytics_calculator.get_analysis_summary(analysis_results)
    if analysis_summary:
        print(f"📈 平均夏普比率: {analysis_summary['average_metrics']['sharpe_ratio']:.2f}")
        print(f"📊 平均波动率: {analysis_summary['average_metrics']['volatility']*100:.1f}%")
        print(f"🎯 平均RSI: {analysis_summary['average_metrics']['rsi']:.1f}")
    
    # 3. 投资决策阶段
    print(f"\n🤖 第三阶段：AI投资决策")
    print("─" * 30)
    
    # 准备持有基金信息
    owned_funds_info = {}
    for fund in config_manager.owned_funds:
        owned_funds_info[fund.code] = {
            'purchase_price': fund.purchase_price,
            'purchase_date': fund.purchase_date
        }
    
    # 批量决策
    decisions = decision_engine.batch_decide(analysis_results, owned_funds_info)
    
    print(f"✅ 投资决策完成: {len(decisions)} 只基金")
    
    # 显示决策摘要
    decision_summary = decision_engine.get_decision_summary(decisions)
    if decision_summary:
        dist = decision_summary['decision_distribution']
        print(f"📋 决策分布: 买入 {dist.get('buy', 0)} | 持有 {dist.get('hold', 0)} | 卖出 {dist.get('sell', 0)}")
        print(f"🎯 平均信心度: {decision_summary['average_confidence']*100:.1f}%")
    
    # 4. 详细报告展示
    print(f"\n📋 第四阶段：详细投资建议")
    print("═" * 60)
    
    # 按决策类型分组显示
    buy_decisions = []
    sell_decisions = []
    hold_decisions = []
    
    for decision in decisions.values():
        if decision.decision_type.value == 'buy':
            buy_decisions.append(decision)
        elif decision.decision_type.value == 'sell':
            sell_decisions.append(decision)
        else:
            hold_decisions.append(decision)
    
    # 显示买入建议
    if buy_decisions:
        print(f"\n🟢 买入建议 ({len(buy_decisions)} 只)")
        print("─" * 40)
        for decision in sorted(buy_decisions, key=lambda x: x.confidence, reverse=True):
            analysis = analysis_results[decision.fund_code]
            fund_info = config_manager.get_fund_by_code(decision.fund_code)
            status = "持有" if config_manager.is_owned_fund(decision.fund_code) else "关注"
            
            print(f"\n📈 {decision.fund_code} - {decision.fund_name} ({status})")
            print(f"   💰 当前价格: ¥{analysis.current_price:.4f}")
            print(f"   🎯 信心度: {decision.confidence*100:.1f}%")
            if decision.target_price:
                print(f"   🎯 目标价格: ¥{decision.target_price:.4f}")
            if decision.stop_loss:
                print(f"   🛡️  止损价格: ¥{decision.stop_loss:.4f}")
            print(f"   📊 技术指标: RSI {analysis.rsi:.1f} | 波动率 {analysis.volatility*100:.1f}%")
            print(f"   📝 决策理由:")
            for reason in decision.reasoning:
                print(f"      • {reason}")
    
    # 显示卖出建议
    if sell_decisions:
        print(f"\n🔴 卖出建议 ({len(sell_decisions)} 只)")
        print("─" * 40)
        for decision in sorted(sell_decisions, key=lambda x: x.confidence, reverse=True):
            analysis = analysis_results[decision.fund_code]
            
            print(f"\n📉 {decision.fund_code} - {decision.fund_name}")
            print(f"   💰 当前价格: ¥{analysis.current_price:.4f}")
            print(f"   🎯 信心度: {decision.confidence*100:.1f}%")
            print(f"   📊 技术指标: RSI {analysis.rsi:.1f} | 波动率 {analysis.volatility*100:.1f}%")
            print(f"   📝 决策理由:")
            for reason in decision.reasoning:
                print(f"      • {reason}")
    
    # 显示持有建议
    if hold_decisions:
        print(f"\n🟡 持有建议 ({len(hold_decisions)} 只)")
        print("─" * 40)
        for decision in sorted(hold_decisions, key=lambda x: x.confidence, reverse=True):
            analysis = analysis_results[decision.fund_code]
            fund_info = config_manager.get_fund_by_code(decision.fund_code)
            status = "持有" if config_manager.is_owned_fund(decision.fund_code) else "关注"
            
            print(f"\n📊 {decision.fund_code} - {decision.fund_name} ({status})")
            print(f"   💰 当前价格: ¥{analysis.current_price:.4f}")
            print(f"   🎯 信心度: {decision.confidence*100:.1f}%")
            print(f"   📊 技术指标: RSI {analysis.rsi:.1f} | 波动率 {analysis.volatility*100:.1f}%")
            print(f"   📝 决策理由:")
            for reason in decision.reasoning:
                print(f"      • {reason}")
    
    # 5. 总结
    print(f"\n🎯 分析总结")
    print("═" * 60)
    print(f"📊 本次分析了 {len(analysis_results)} 只基金")
    print(f"🤖 AI 提供了 {len(decisions)} 个投资建议")
    print(f"⏰ 分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"💡 建议: 请结合个人风险承受能力和投资目标做出最终决策")
    print("═" * 60)
    
    logger.info("完整分析流程执行完成！")


if __name__ == "__main__":
    main()
