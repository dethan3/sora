"""
定时任务调度器
负责管理数据更新、分析执行、报告生成等定时任务
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any
import threading
import time
from loguru import logger
from dataclasses import dataclass
from enum import Enum

from ..config.manager import ConfigManager
from ..data.fetcher import DataFetcher
from ..analytics.calculator import AnalyticsCalculator
from ..data.cache import DataCache


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskType(Enum):
    """任务类型枚举"""
    DATA_UPDATE = "data_update"
    ANALYSIS = "analysis"
    REPORT = "report"
    CLEANUP = "cleanup"


@dataclass
class ScheduledTask:
    """定时任务数据类"""
    task_id: str
    task_type: TaskType
    name: str
    description: str
    schedule_time: datetime
    interval_minutes: Optional[int] = None  # 间隔分钟数，None表示一次性任务
    status: TaskStatus = TaskStatus.PENDING
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    run_count: int = 0
    max_runs: Optional[int] = None  # 最大运行次数，None表示无限制
    callback: Optional[Callable] = None
    kwargs: Optional[Dict[str, Any]] = None
    created_at: datetime = None
    updated_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()
        if self.next_run is None:
            self.next_run = self.schedule_time


class TaskScheduler:
    """定时任务调度器"""
    
    def __init__(self, config_manager: ConfigManager):
        """初始化调度器"""
        self.config_manager = config_manager
        self.data_fetcher = DataFetcher(config_manager)
        self.analytics = AnalyticsCalculator(config_manager)
        self.cache = DataCache(
            cache_dir=config_manager.system.cache_dir,
            expire_hours=24,
            max_cache_size_mb=100
        )
        
        self.tasks: Dict[str, ScheduledTask] = {}
        self.running = False
        self.scheduler_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        
        # 从配置加载调度器设置
        self.scheduler_config = config_manager.scheduler
        
        logger.info("任务调度器初始化完成")
    
    def add_task(self, task: ScheduledTask) -> bool:
        """添加定时任务"""
        try:
            if task.task_id in self.tasks:
                logger.warning(f"任务 {task.task_id} 已存在，将被覆盖")
            
            self.tasks[task.task_id] = task
            logger.info(f"添加定时任务: {task.name} ({task.task_id})")
            logger.info(f"下次执行时间: {task.next_run}")
            
            return True
        except Exception as e:
            logger.error(f"添加任务失败: {str(e)}")
            return False
    
    def remove_task(self, task_id: str) -> bool:
        """移除定时任务"""
        try:
            if task_id in self.tasks:
                task = self.tasks.pop(task_id)
                logger.info(f"移除定时任务: {task.name} ({task_id})")
                return True
            else:
                logger.warning(f"任务 {task_id} 不存在")
                return False
        except Exception as e:
            logger.error(f"移除任务失败: {str(e)}")
            return False
    
    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """获取任务信息"""
        return self.tasks.get(task_id)
    
    def list_tasks(self) -> List[ScheduledTask]:
        """列出所有任务"""
        return list(self.tasks.values())
    
    def start(self) -> bool:
        """启动调度器"""
        try:
            if self.running:
                logger.warning("调度器已在运行中")
                return True
            
            self.running = True
            self.stop_event.clear()
            
            # 创建并启动调度器线程
            self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
            self.scheduler_thread.start()
            
            logger.info("任务调度器启动成功")
            return True
        except Exception as e:
            logger.error(f"启动调度器失败: {str(e)}")
            self.running = False
            return False
    
    def stop(self) -> bool:
        """停止调度器"""
        try:
            if not self.running:
                logger.warning("调度器未在运行")
                return True
            
            self.running = False
            self.stop_event.set()
            
            # 等待调度器线程结束
            if self.scheduler_thread and self.scheduler_thread.is_alive():
                self.scheduler_thread.join(timeout=5)
                if self.scheduler_thread.is_alive():
                    logger.warning("调度器线程未能正常结束")
            
            logger.info("任务调度器已停止")
            return True
        except Exception as e:
            logger.error(f"停止调度器失败: {str(e)}")
            return False
    
    def _run_scheduler(self):
        """调度器主循环"""
        logger.info("调度器主循环开始")
        
        while self.running and not self.stop_event.is_set():
            try:
                current_time = datetime.now()
                
                # 检查并执行到期任务
                for task_id, task in list(self.tasks.items()):
                    if self._should_run_task(task, current_time):
                        self._execute_task(task)
                
                # 休眠一段时间再继续检查
                time.sleep(30)  # 每30秒检查一次
                
            except Exception as e:
                logger.error(f"调度器运行错误: {str(e)}")
                time.sleep(60)  # 出错时休眠1分钟
        
        logger.info("调度器主循环结束")
    
    def _should_run_task(self, task: ScheduledTask, current_time: datetime) -> bool:
        """判断任务是否应该执行"""
        # 检查任务状态
        if task.status == TaskStatus.RUNNING:
            return False
        
        if task.status == TaskStatus.CANCELLED:
            return False
        
        # 检查是否到达执行时间
        if task.next_run and current_time >= task.next_run:
            # 检查最大运行次数
            if task.max_runs and task.run_count >= task.max_runs:
                logger.info(f"任务 {task.name} 已达到最大运行次数 {task.max_runs}")
                task.status = TaskStatus.COMPLETED
                return False
            
            return True
        
        return False
    
    def _execute_task(self, task: ScheduledTask):
        """执行任务"""
        logger.info(f"开始执行任务: {task.name} ({task.task_id})")
        
        task.status = TaskStatus.RUNNING
        task.last_run = datetime.now()
        task.run_count += 1
        task.updated_at = datetime.now()
        
        try:
            # 根据任务类型执行相应操作
            if task.task_type == TaskType.DATA_UPDATE:
                self._execute_data_update_task(task)
            elif task.task_type == TaskType.ANALYSIS:
                self._execute_analysis_task(task)
            elif task.task_type == TaskType.REPORT:
                self._execute_report_task(task)
            elif task.task_type == TaskType.CLEANUP:
                self._execute_cleanup_task(task)
            else:
                # 执行自定义回调函数
                if task.callback:
                    kwargs = task.kwargs or {}
                    task.callback(**kwargs)
            
            task.status = TaskStatus.COMPLETED
            logger.info(f"任务执行完成: {task.name}")
            
        except Exception as e:
            task.status = TaskStatus.FAILED
            logger.error(f"任务执行失败: {task.name} - {str(e)}")
        
        # 计算下次执行时间
        if task.interval_minutes:
            task.next_run = datetime.now() + timedelta(minutes=task.interval_minutes)
            task.status = TaskStatus.PENDING
            logger.info(f"任务 {task.name} 下次执行时间: {task.next_run}")
    
    def _execute_data_update_task(self, task: ScheduledTask):
        """执行数据更新任务 - 优化版本，使用批量获取减少API调用"""
        kwargs = task.kwargs or {}
        fund_codes = kwargs.get('fund_codes', [])
        
        if not fund_codes:
            # 获取所有启用的基金代码
            fund_codes = []
            for fund in self.config_manager.priority_funds:
                if fund.enabled:
                    fund_codes.append(fund.code)
            
            if hasattr(self.config_manager, 'region_funds') and self.config_manager.region_funds:
                for region, funds in self.config_manager.region_funds.items():
                    for fund in funds:
                        if hasattr(fund, 'enabled') and fund.enabled:
                            fund_codes.append(fund.code)
        
        # 去重
        fund_codes = list(set(fund_codes))
        logger.info(f"开始更新 {len(fund_codes)} 只基金的数据（优化批量获取）")
        
        try:
            # 使用优化后的批量获取方法
            fund_data_results = self.data_fetcher.batch_get_current_data(fund_codes)
            
            # 批量缓存数据
            success_count = 0
            for fund_code, fund_data in fund_data_results.items():
                try:
                    # 缓存当前数据
                    self.cache.save_fund_data(fund_code, fund_data)
                    
                    # 获取历史数据（仅为优先级基金）
                    if self._is_priority_fund(fund_code):
                        try:
                            historical_data = self.data_fetcher.get_historical_data(fund_code)
                            if historical_data:
                                self.cache.save_historical_data(fund_code, historical_data)
                                logger.debug(f"基金 {fund_code} 历史数据更新成功")
                        except Exception as hist_error:
                            logger.warning(f"基金 {fund_code} 历史数据获取失败: {hist_error}")
                    
                    success_count += 1
                    logger.debug(f"基金 {fund_code} 数据更新成功")
                    
                except Exception as cache_error:
                    logger.error(f"缓存基金 {fund_code} 数据失败: {cache_error}")
            
            logger.info(f"数据更新任务完成: {success_count}/{len(fund_codes)} 只基金成功")
            
        except Exception as e:
            logger.error(f"批量数据更新失败: {str(e)}")
            # 回退到逐个更新模式
            self._execute_data_update_fallback(fund_codes)
    
    def _is_priority_fund(self, fund_code: str) -> bool:
        """判断是否为优先级基金"""
        for fund in self.config_manager.priority_funds:
            if fund.code == fund_code:
                return True
        return False
    
    def _execute_data_update_fallback(self, fund_codes: List[str]):
        """数据更新的回退方案（逐个获取）"""
        logger.info(f"使用回退方案逐个更新 {len(fund_codes)} 只基金")
        
        success_count = 0
        for fund_code in fund_codes:
            try:
                # 获取当前数据
                fund_data = self.data_fetcher.get_current_data(fund_code)
                if fund_data:
                    self.cache.save_fund_data(fund_code, fund_data)
                    success_count += 1
                    logger.debug(f"基金 {fund_code} 数据更新成功")
                else:
                    logger.warning(f"基金 {fund_code} 数据获取失败")
                    
            except Exception as e:
                logger.error(f"更新基金 {fund_code} 数据失败: {str(e)}")
        
        logger.info(f"回退方案完成: {success_count}/{len(fund_codes)} 只基金成功")
    
    def _execute_analysis_task(self, task: ScheduledTask):
        """执行分析任务"""
        kwargs = task.kwargs or {}
        fund_codes = kwargs.get('fund_codes', [])
        
        if not fund_codes:
            # 获取所有启用的基金代码
            fund_codes = []
            for fund in self.config_manager.priority_funds:
                if fund.enabled:
                    fund_codes.append(fund.code)
                    
        logger.info(f"开始分析 {len(fund_codes)} 只基金")
        
        analysis_results = {}
        for fund_code in fund_codes:
            try:
                # 获取基金数据
                fund_data = self.cache.get_fund_data(fund_code)
                if fund_data:
                    # 执行技术分析
                    result = self.analytics.analyze_fund(fund_code, fund_data)
                    analysis_results[fund_code] = result
                    
                    # 缓存分析结果
                    self.cache.save_analysis_result(fund_code, result)
                    logger.debug(f"基金 {fund_code} 分析完成")
                else:
                    logger.warning(f"基金 {fund_code} 数据不可用，跳过分析")
                    
            except Exception as e:
                logger.error(f"分析基金 {fund_code} 失败: {str(e)}")
        
        logger.info(f"分析任务完成，成功分析 {len(analysis_results)} 只基金")
    
    def _execute_report_task(self, task: ScheduledTask):
        """执行报告生成任务"""
        logger.info("开始生成分析报告")
        
        try:
            # 获取所有分析结果
            all_results = {}
            for fund in self.config_manager.priority_funds:
                if fund.enabled:
                    result = self.cache.get_analysis_result(fund.code)
                    if result:
                        all_results[fund.code] = result
            
            # 生成报告
            report = self._generate_summary_report(all_results)
            
            # 保存报告
            report_path = f"data/reports/analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            self.cache.save_report(report, report_path)
            
            logger.info(f"分析报告生成完成: {report_path}")
            
        except Exception as e:
            logger.error(f"生成报告失败: {str(e)}")
    
    def _execute_cleanup_task(self, task: ScheduledTask):
        """执行清理任务"""
        logger.info("开始执行数据清理")
        
        try:
            # 清理过期缓存
            self.cache.cleanup_expired_data()
            logger.info("数据清理任务完成")
            
        except Exception as e:
            logger.error(f"数据清理失败: {str(e)}")
    
    def _generate_summary_report(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """生成汇总报告"""
        report = {
            "generated_at": datetime.now().isoformat(),
            "total_funds": len(analysis_results),
            "summary": {
                "strong_buy": 0,
                "buy": 0,
                "hold": 0,
                "sell": 0,
                "strong_sell": 0
            },
            "fund_recommendations": {},
            "top_performers": [],
            "risk_alerts": []
        }
        
        for fund_code, result in analysis_results.items():
            if result and 'recommendation' in result:
                recommendation = result['recommendation']
                report["fund_recommendations"][fund_code] = recommendation
                
                # 统计各类推荐
                if recommendation in report["summary"]:
                    report["summary"][recommendation] += 1
        
        return report
    
    def setup_default_tasks(self):
        """设置默认定时任务"""
        try:
            # ETF缓存刷新任务（每6小时刷新一次）
            etf_cache_refresh_task = ScheduledTask(
                task_id="etf_cache_refresh",
                task_type=TaskType.DATA_UPDATE,
                name="ETF列表缓存刷新",
                description="定期刷新ETF列表缓存以保证数据新鲜性",
                schedule_time=datetime.now() + timedelta(minutes=30),  # 30分钟后开始
                interval_minutes=6 * 60,  # 每6小时执行一次
                callback=self._refresh_etf_cache_callback
            )
            self.add_task(etf_cache_refresh_task)
            
            # 每日数据更新任务（优化版本）
            if self.scheduler_config.data_update_enabled:
                data_update_task = ScheduledTask(
                    task_id="daily_data_update",
                    task_type=TaskType.DATA_UPDATE,
                    name="每日数据更新（优化）",
                    description="批量获取监控基金的最新数据，减少API调用",
                    schedule_time=datetime.now() + timedelta(minutes=1),  # 1分钟后开始
                    interval_minutes=self.scheduler_config.data_update_interval * 60,  # 转换为分钟
                )
                self.add_task(data_update_task)
            
            # 每日分析任务
            if self.scheduler_config.analysis_enabled:
                analysis_task = ScheduledTask(
                    task_id="daily_analysis",
                    task_type=TaskType.ANALYSIS,
                    name="每日量化分析",
                    description="对所有监控基金进行技术分析",
                    schedule_time=datetime.now() + timedelta(minutes=5),  # 5分钟后开始
                    interval_minutes=self.scheduler_config.analysis_interval * 60,  # 转换为分钟
                )
                self.add_task(analysis_task)
            
            # 每周报告生成任务
            if self.scheduler_config.report_enabled:
                report_task = ScheduledTask(
                    task_id="weekly_report",
                    task_type=TaskType.REPORT,
                    name="每周分析报告",
                    description="生成每周分析汇总报告",
                    schedule_time=datetime.now() + timedelta(minutes=10),  # 10分钟后开始
                    interval_minutes=self.scheduler_config.report_interval * 60,  # 转换为分钟
                )
                self.add_task(report_task)
            
            # 数据清理任务
            cleanup_task = ScheduledTask(
                task_id="data_cleanup",
                task_type=TaskType.CLEANUP,
                name="数据清理",
                description="清理过期的缓存数据",
                schedule_time=datetime.now() + timedelta(hours=1),  # 1小时后开始
                interval_minutes=24 * 60,  # 每天执行一次
            )
            self.add_task(cleanup_task)
            
            logger.info("默认定时任务设置完成")
            
        except Exception as e:
            logger.error(f"设置默认任务失败: {str(e)}")
    
    def _refresh_etf_cache_callback(self):
        """刷新ETF列表缓存的回调函数"""
        try:
            logger.info("开始刷新ETF列表缓存")
            success = self.data_fetcher.refresh_etf_list_cache()
            if success:
                logger.info("ETF列表缓存刷新成功")
            else:
                logger.error("ETF列表缓存刷新失败")
        except Exception as e:
            logger.error(f"ETF列表缓存刷新过程中出错: {e}")
    
    def add_priority_fund_task(self, fund_code: str, task_name: str = None):
        """
        为特定优先级基金添加更频繁的数据更新任务
        
        Args:
            fund_code: 基金代码
            task_name: 任务名称，不提供则自动生成
        """
        if not task_name:
            task_name = f"优先级基金 {fund_code} 数据更新"
        
        task_id = f"priority_fund_{fund_code}"
        
        priority_task = ScheduledTask(
            task_id=task_id,
            task_type=TaskType.DATA_UPDATE,
            name=task_name,
            description=f"高频率更新优先级基金 {fund_code} 的数据",
            schedule_time=datetime.now() + timedelta(minutes=5),
            interval_minutes=30,  # 每30分钟更新一次
            kwargs={'fund_codes': [fund_code]}
        )
        
        self.add_task(priority_task)
        logger.info(f"已为优先级基金 {fund_code} 添加高频率更新任务")
    
    def get_optimization_status(self) -> Dict[str, Any]:
        """获取数据获取优化状态"""
        try:
            # 获取所有关注的基金代码
            all_fund_codes = []
            for fund in self.config_manager.priority_funds:
                if fund.enabled:
                    all_fund_codes.append(fund.code)
            
            if hasattr(self.config_manager, 'region_funds') and self.config_manager.region_funds:
                for region, funds in self.config_manager.region_funds.items():
                    for fund in funds:
                        if hasattr(fund, 'enabled') and fund.enabled:
                            all_fund_codes.append(fund.code)
            
            all_fund_codes = list(set(all_fund_codes))
            
            # 获取数据获取器状态
            data_summary = self.data_fetcher.get_data_summary(all_fund_codes)
            
            return {
                'total_monitored_funds': len(all_fund_codes),
                'optimization_enabled': True,
                'data_fetcher_status': data_summary,
                'scheduler_running': self.running,
                'last_update': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"获取优化状态失败: {e}")
            return {
                'error': str(e),
                'optimization_enabled': False
            }
    
    def get_status_summary(self) -> Dict[str, Any]:
        """获取调度器状态摘要"""
        task_status_count = {}
        for status in TaskStatus:
            task_status_count[status.value] = 0
        
        for task in self.tasks.values():
            task_status_count[task.status.value] += 1
        
        return {
            "scheduler_running": self.running,
            "total_tasks": len(self.tasks),
            "task_status": task_status_count,
            "next_task": self._get_next_task_info(),
            "last_update": datetime.now().isoformat()
        }
    
    def _get_next_task_info(self) -> Optional[Dict[str, Any]]:
        """获取下一个要执行的任务信息"""
        next_task = None
        next_time = None
        
        for task in self.tasks.values():
            if task.status == TaskStatus.PENDING and task.next_run:
                if next_time is None or task.next_run < next_time:
                    next_time = task.next_run
                    next_task = task
        
        if next_task:
            return {
                "task_id": next_task.task_id,
                "name": next_task.name,
                "next_run": next_task.next_run.isoformat(),
                "task_type": next_task.task_type.value
            }
        
        return None
