#!/usr/bin/env python3
"""
Sora - ETF 量化分析平台

专注中国市场ETF的数据驱动量化投资分析平台
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
from rich.console import Console
from rich.table import Table

console = Console()

@click.group()
@click.version_option(version="2.0.0", prog_name="Sora ETF量化分析平台")
def cli():
    """🚀 Sora - ETF 量化分析平台
    
    专注中国市场ETF的数据驱动量化投资分析平台
    """
    setup_logging()

@cli.command()
def init():
    """🔧 初始化系统配置和数据库"""
    console.print("[bold green]🔧 正在初始化 Sora ETF量化分析平台...[/bold green]")
    
    try:
        from src.utils.initializer import SystemInitializer
        
        initializer = SystemInitializer()
        success = initializer.initialize()
        
        if success:
            # Initialize database via StorageService (best-effort)
            try:
                from src.storage.service import StorageService
                StorageService().init_db()
                console.print("[green]🗄️ 数据库已初始化[/green]")
            except Exception as db_e:  # noqa: PIE786
                logger.warning(f"数据库初始化跳过或失败（占位继续）: {db_e}")
            
            console.print("[bold green]✅ 系统初始化完成！[/bold green]")
            console.print("[yellow]📋 下一步：运行 'python main.py analyze' 开始分析[/yellow]")
        else:
            console.print("[bold red]❌ 系统初始化失败[/bold red]")
            sys.exit(1)
            
    except ImportError as e:
        logger.warning(f"可选模块缺失，使用占位初始化: {e}")
        console.print("[yellow]⚠️ 未找到完整的初始化模块，已跳过实际初始化步骤（占位返回成功）。[/yellow]")
        # Try to init DB anyway if storage is available
        try:
            from src.storage.service import StorageService
            StorageService().init_db()
            console.print("[green]🗄️ 数据库已初始化（在占位模式下）[/green]")
        except Exception as db_e:  # noqa: PIE786
            logger.warning(f"数据库初始化跳过或失败（占位继续）: {db_e}")
        return
    except Exception as e:
        logger.error(f"初始化失败: {e}")
        console.print(f"[bold red]❌ 初始化失败: {e}[/bold red]")
        sys.exit(1)

@cli.command()
@click.option('--force-update', '-f', is_flag=True, help='强制更新所有数据')
@click.option('--etf-code', '-e', help='分析特定ETF代码')
def analyze(force_update: bool, etf_code: str):
    """📊 运行ETF量化分析"""
    console.print("[bold blue]📊 启动ETF量化分析...[/bold blue]")
    
    try:
        from src.core.analyzer import ETFAnalyzer
        
        analyzer = ETFAnalyzer()
        
        if etf_code:
            console.print(f"[yellow]🎯 分析特定ETF: {etf_code}[/yellow]")
            results = analyzer.analyze_single(etf_code, force_update)
        else:
            console.print("[yellow]📈 分析所有ETF基金...[/yellow]")
            results = analyzer.analyze_all(force_update)
        
        if results:
            console.print("[bold green]✅ 分析完成！[/bold green]")
            display_analysis_summary(results)
        else:
            console.print("[bold red]❌ 分析失败[/bold red]")
            
    except ImportError as e:
        logger.warning(f"Analyzer 模块缺失，输出占位结果: {e}")
        placeholder = {
            'total_etfs': 0 if etf_code else 3,
            'successful_fetches': 0,
            'buy_signals': 0,
            'sell_signals': 0,
            'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        console.print("[yellow]⚠️ Analyzer 未就绪，返回占位分析结果。[/yellow]")
        display_analysis_summary(placeholder)
        return
    except Exception as e:
        logger.error(f"分析失败: {e}")
        console.print(f"[bold red]❌ 分析失败: {e}[/bold red]")
        sys.exit(1)

@cli.command()
@click.option('--format', '-f', type=click.Choice(['table', 'excel', 'json']), 
              default='table', help='报告格式')
@click.option('--days', '-d', default=7, help='显示最近N天的建议')
def report(format: str, days: int):
    """📋 生成投资建议报告"""
    console.print("[bold cyan]📋 生成投资建议报告...[/bold cyan]")
    
    try:
        from src.core.reporter import ETFReporter
        
        reporter = ETFReporter()
        report_data = reporter.generate_report(days)
        
        if format == 'table':
            display_investment_recommendations(report_data)
        elif format == 'excel':
            file_path = reporter.export_excel(report_data)
            console.print(f"[green]📊 Excel报告已保存: {file_path}[/green]")
        elif format == 'json':
            file_path = reporter.export_json(report_data)
            console.print(f"[green]📄 JSON报告已保存: {file_path}[/green]")
            
    except ImportError as e:
        logger.warning(f"Reporter 模块缺失，输出占位报告: {e}")
        placeholder = {
            'buy_recommendations': [],
            'sell_recommendations': []
        }
        console.print("[yellow]⚠️ Reporter 未就绪，显示占位报告。[/yellow]")
        display_investment_recommendations(placeholder)
        return
    except Exception as e:
        logger.error(f"报告生成失败: {e}")
        console.print(f"[bold red]❌ 报告生成失败: {e}[/bold red]")
        sys.exit(1)

@cli.command()
@click.option('--weekly', is_flag=True, help='启动每周定时分析')
@click.option('--stop', is_flag=True, help='停止定时任务')
def schedule(weekly: bool, stop: bool):
    """⏰ 管理定时任务"""
    
    try:
        from src.scheduler.task_manager import TaskManager
        
        task_manager = TaskManager()
        
        if stop:
            console.print("[yellow]⏹️ 停止所有定时任务...[/yellow]")
            task_manager.stop_all()
            console.print("[green]✅ 定时任务已停止[/green]")
            
        elif weekly:
            console.print("[blue]⏰ 启动每周定时分析...[/blue]")
            task_manager.start_weekly_analysis()
            console.print("[green]✅ 每周定时任务已启动（每周一 09:00）[/green]")
            console.print("[yellow]💡 任务将在后台运行，使用 --stop 停止[/yellow]")
            
        else:
            # 显示当前任务状态
            status = task_manager.get_status()
            display_schedule_status(status)
            
    except ImportError as e:
        logger.warning(f"TaskManager 模块缺失，显示占位任务状态: {e}")
        status = { 'tasks': [ { 'name': 'weekly_analysis', 'status': 'stopped', 'next_run': 'N/A' } ] }
        display_schedule_status(status)
        return
    except Exception as e:
        logger.error(f"任务管理失败: {e}")
        console.print(f"[bold red]❌ 任务管理失败: {e}[/bold red]")
        sys.exit(1)

@cli.command()
@click.option('--days', '-d', default=30, help='回测天数')
@click.option('--strategy', '-s', default='default', help='回测策略')
def backtest(days: int, strategy: str):
    """🔄 历史数据回测"""
    console.print(f"[bold magenta]🔄 启动{days}天历史回测（策略: {strategy}）...[/bold magenta]")
    
    try:
        from src.core.backtester import ETFBacktester
        
        backtester = ETFBacktester()
        results = backtester.run_backtest(days, strategy)
        
        if results:
            display_backtest_results(results)
        else:
            console.print("[bold red]❌ 回测失败[/bold red]")
            
    except ImportError as e:
        logger.warning(f"Backtester 模块缺失，输出占位回测结果: {e}")
        placeholder = {
            'total_return': 0.0,
            'annual_return': 0.0,
            'max_drawdown': 0.0,
            'sharpe_ratio': 0.0,
            'win_rate': 0.0
        }
        console.print("[yellow]⚠️ Backtester 未就绪，显示占位回测结果。[/yellow]")
        display_backtest_results(placeholder)
        return
    except Exception as e:
        logger.error(f"回测失败: {e}")
        console.print(f"[bold red]❌ 回测失败: {e}[/bold red]")
        sys.exit(1)

@cli.command()
def status():
    """📊 显示系统状态"""
    console.print("[bold blue]📊 系统状态检查...[/bold blue]")
    
    try:
        from src.utils.health_checker import HealthChecker
        
        health_checker = HealthChecker()
        status = health_checker.check_all()
        
        display_system_status(status)
        
    except ImportError as e:
        logger.warning(f"HealthChecker 模块缺失，显示占位系统状态: {e}")
        status = {
            'environment': { 'healthy': True, 'status': 'OK', 'details': 'Placeholders active' },
            'storage': { 'healthy': False, 'status': 'Not initialized', 'details': 'Storage module pending' }
        }
        display_system_status(status)
        return
    except Exception as e:
        logger.error(f"状态检查失败: {e}")
        console.print(f"[bold red]❌ 状态检查失败: {e}[/bold red]")

def setup_logging():
    """设置日志系统"""
    # 移除默认处理器
    logger.remove()
    
    # 创建日志目录
    log_dir = Path("data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # 添加控制台输出（简化格式）
    logger.add(
        sys.stderr,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        colorize=True
    )
    
    # 添加文件输出（详细格式）
    logger.add(
        "data/logs/sora.log",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        rotation="10 MB",
        retention="30 days",
        compression="zip"
    )

def display_analysis_summary(results):
    """显示分析摘要"""
    table = Table(title="📊 ETF分析摘要")
    
    table.add_column("指标", style="cyan")
    table.add_column("数值", style="magenta")
    
    table.add_row("分析ETF数量", str(results.get('total_etfs', 0)))
    table.add_row("成功获取数据", str(results.get('successful_fetches', 0)))
    table.add_row("生成买入信号", str(results.get('buy_signals', 0)))
    table.add_row("生成卖出信号", str(results.get('sell_signals', 0)))
    table.add_row("分析时间", results.get('analysis_time', 'N/A'))
    
    console.print(table)

def display_investment_recommendations(report_data):
    """显示投资建议"""
    # 买入建议
    if report_data.get('buy_recommendations'):
        buy_table = Table(title="🟢 买入建议", show_header=True)
        buy_table.add_column("ETF代码", style="cyan")
        buy_table.add_column("名称", style="white")
        buy_table.add_column("当前价格", style="green")
        buy_table.add_column("信号强度", style="yellow")
        buy_table.add_column("建议理由", style="dim")
        
        for rec in report_data['buy_recommendations']:
            buy_table.add_row(
                rec['code'],
                rec['name'],
                f"¥{rec['price']:.2f}",
                rec['signal_strength'],
                rec['reason']
            )
        
        console.print(buy_table)
    
    # 卖出建议
    if report_data.get('sell_recommendations'):
        sell_table = Table(title="🔴 卖出建议", show_header=True)
        sell_table.add_column("ETF代码", style="cyan")
        sell_table.add_column("名称", style="white")
        sell_table.add_column("当前价格", style="red")
        sell_table.add_column("信号强度", style="yellow")
        sell_table.add_column("建议理由", style="dim")
        
        for rec in report_data['sell_recommendations']:
            sell_table.add_row(
                rec['code'],
                rec['name'],
                f"¥{rec['price']:.2f}",
                rec['signal_strength'],
                rec['reason']
            )
        
        console.print(sell_table)

def display_schedule_status(status):
    """显示定时任务状态"""
    table = Table(title="⏰ 定时任务状态")
    
    table.add_column("任务", style="cyan")
    table.add_column("状态", style="magenta")
    table.add_column("下次执行", style="yellow")
    
    for task in status.get('tasks', []):
        table.add_row(
            task['name'],
            task['status'],
            task['next_run']
        )
    
    console.print(table)

def display_backtest_results(results):
    """显示回测结果"""
    table = Table(title="🔄 回测结果")
    
    table.add_column("指标", style="cyan")
    table.add_column("数值", style="magenta")
    
    table.add_row("总收益率", f"{results.get('total_return', 0):.2%}")
    table.add_row("年化收益率", f"{results.get('annual_return', 0):.2%}")
    table.add_row("最大回撤", f"{results.get('max_drawdown', 0):.2%}")
    table.add_row("夏普比率", f"{results.get('sharpe_ratio', 0):.2f}")
    table.add_row("胜率", f"{results.get('win_rate', 0):.2%}")
    
    console.print(table)

def display_system_status(status):
    """显示系统状态"""
    table = Table(title="📊 系统状态")
    
    table.add_column("组件", style="cyan")
    table.add_column("状态", style="magenta")
    table.add_column("详情", style="dim")
    
    for component, info in status.items():
        status_icon = "✅" if info['healthy'] else "❌"
        table.add_row(
            component,
            f"{status_icon} {info['status']}",
            info.get('details', '')
        )
    
    console.print(table)

if __name__ == "__main__":
    try:
        cli()
    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️ 用户中断操作[/yellow]")
        sys.exit(0)
    except Exception as e:
        logger.error(f"程序异常退出: {e}")
        console.print(f"[bold red]💥 程序异常退出: {e}[/bold red]")
        sys.exit(1)
