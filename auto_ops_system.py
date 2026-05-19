"""
自动化运维与自愈合系统 (多Agent协作、闭环验证)
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Dict, Any
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)-15s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("AutoOps")


# -------------------- 1. 模拟的被管系统 --------------------
class SimulatedService:
    """模拟一个简单的 Web 服务，具有可调整的响应延迟和连接池状态。"""
    def __init__(self):
        self.base_latency_ms = 50        # 基础延迟
        self.db_pool_size = 10           # 数据库连接池大小
        self.active_connections = 0      # 当前活跃连接
        self.normal_pool_size = 10       # 正常池大小（用于重置）
        # 服务是否被手动停止（模拟不可用）
        self.service_up = True

    async def handle_request(self) -> float:
        """处理一个请求，返回耗时(ms)。模拟连接池竞争导致延迟升高。"""
        if not self.service_up:
            return float('inf')  # 服务不可用

        # 模拟数据库查询：需要获取连接
        if self.active_connections >= self.db_pool_size:
            # 连接池满，等待连接释放（模拟高延迟）
            wait_time = random.uniform(200, 500)
            await asyncio.sleep(wait_time / 1000)  # 转换为秒
            latency = self.base_latency_ms + wait_time + random.uniform(20, 80)
        else:
            self.active_connections += 1
            # 正常查询耗时
            query_time = random.uniform(10, 30)
            await asyncio.sleep(query_time / 1000)
            self.active_connections -= 1
            latency = self.base_latency_ms + query_time + random.uniform(0, 10)
        return latency

    def reset(self):
        """重置到正常状态"""
        self.db_pool_size = self.normal_pool_size
        self.service_up = True
        self.active_connections = 0


# -------------------- 2. 数据模型 --------------------
class AlertLevel(Enum):
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"

@dataclass
class Metric:
    timestamp: float
    avg_latency_ms: float
    error_rate: float
    pool_usage: float  # 连接池使用率

@dataclass
class Alert:
    level: AlertLevel
    message: str
    metrics: Metric
    source: str = "MonitorAgent"

@dataclass
class RepairPlan:
    action: str          # 修复动作标识
    description: str
    params: Dict[str, Any] = field(default_factory=dict)
    is_rollback: bool = False

@dataclass
class VerificationResult:
    success: bool
    details: str


# -------------------- 3. Agent 基类 --------------------
class BaseAgent:
    def __init__(self, name: str, queue: asyncio.Queue):
        self.name = name
        self.queue = queue
        self.logger = logging.getLogger(name)

    async def send_message(self, msg_type: str, payload: Any):
        await self.queue.put({"type": msg_type, "payload": payload, "sender": self.name})

    async def run(self):
        """子类需要实现的主循环"""
        raise NotImplementedError


# -------------------- 4. MonitorAgent --------------------
class MonitorAgent(BaseAgent):
    def __init__(self, service: SimulatedService, queue: asyncio.Queue, threshold_ms=200):
        super().__init__("MonitorAgent", queue)
        self.service = service
        self.threshold_ms = threshold_ms
        self.alert_sent = False  # 防止重复发送同一告警

    async def collect_metrics(self) -> Metric:
        # 模拟采样：发送5个并发请求，计算平均延迟和错误率
        latencies = []
        errors = 0
        for _ in range(5):
            lat = await self.service.handle_request()
            if lat == float('inf'):
                errors += 1
            else:
                latencies.append(lat)
        avg_lat = sum(latencies) / len(latencies) if latencies else float('inf')
        error_rate = errors / 5
        pool_usage = self.service.active_connections / self.service.db_pool_size if self.service.db_pool_size > 0 else 0
        return Metric(time.time(), avg_lat, error_rate, pool_usage)

    async def run(self):
        self.logger.info("开始监控...")
        while True:
            metric = await self.collect_metrics()
            self.logger.debug(f"指标: avg_lat={metric.avg_latency_ms:.1f}ms, err_rate={metric.error_rate:.1%}, pool_usage={metric.pool_usage:.1%}")

            # 判断异常
            if metric.avg_latency_ms > self.threshold_ms or metric.error_rate > 0.5:
                if not self.alert_sent:
                    level = AlertLevel.CRITICAL if metric.error_rate > 0.5 else AlertLevel.WARNING
                    alert = Alert(level, "服务响应延迟过高或错误率异常", metric)
                    await self.send_message("alert", alert)
                    self.alert_sent = True
                    self.logger.warning(f"发送告警: {alert.message}")
            else:
                if self.alert_sent:
                    # 指标恢复正常，发送恢复通知（可用于清空告警）
                    self.alert_sent = False
                    self.logger.info("指标已恢复正常")
            await asyncio.sleep(2)  # 采集间隔


# -------------------- 5. DiagnosisAgent --------------------
class DiagnosisAgent(BaseAgent):
    """
    诊断Agent：收到告警后分析根因，生成修复计划列表（按优先级排序）。
    支持多种修复策略：
      - 扩容连接池
      - 重启服务
    会记录当前尝试的策略索引，以便在验证失败后切换策略。
    """
    def __init__(self, queue: asyncio.Queue, service: SimulatedService):
        super().__init__("DiagnosisAgent", queue)
        self.service = service
        # 内部状态：记录当前告警对应的修复计划列表和当前尝试的索引
        self.current_plans: List[RepairPlan] = []
        self.current_plan_index = 0
        self.active_alert: Optional[Alert] = None

    async def analyze(self, alert: Alert) -> List[RepairPlan]:
        """根据告警和当前系统状态生成修复计划"""
        metric = alert.metrics
        plans = []
        if metric.pool_usage > 0.8 or metric.avg_latency_ms > 300:
            # 怀疑连接池不足
            plans.append(RepairPlan(
                action="increase_pool",
                description="扩大数据库连接池至20",
                params={"new_size": 20}
            ))
        # 备选方案：重启服务（可能解决内存泄漏、死锁等未知问题）
        plans.append(RepairPlan(
            action="restart_service",
            description="重启应用服务",
            params={}
        ))
        return plans

    async def run(self):
        while True:
            msg = await self.queue.get()
            if msg["type"] == "alert":
                alert = msg["payload"]
                self.logger.info(f"收到告警: {alert.message}")
                self.active_alert = alert
                self.current_plans = await self.analyze(alert)
                self.current_plan_index = 0
                if self.current_plans:
                    # 发送第一个修复计划给HealingAgent
                    plan = self.current_plans[0]
                    self.logger.info(f"诊断完成，首选修复计划: {plan.description}")
                    await self.send_message("repair_plan", plan)
                else:
                    self.logger.error("无可用修复计划，需要人工介入")
                    await self.send_message("escalate", "无自动修复方案")
            elif msg["type"] == "retry_diagnosis":
                # 验证失败后，尝试下一个修复方案
                if self.current_plan_index + 1 < len(self.current_plans):
                    self.current_plan_index += 1
                    plan = self.current_plans[self.current_plan_index]
                    self.logger.info(f"切换到备选修复计划: {plan.description}")
                    await self.send_message("repair_plan", plan)
                else:
                    self.logger.error("所有自动修复方案均失败，升级为人工处理")
                    await self.send_message("escalate", "所有修复尝试无效，需人工介入")
            else:
                # 其他消息暂时忽略
                pass


# -------------------- 6. HealingAgent --------------------
class HealingAgent(BaseAgent):
    """执行修复计划，执行前后记录状态，执行后发起验证请求。"""
    def __init__(self, queue: asyncio.Queue, service: SimulatedService):
        super().__init__("HealingAgent", queue)
        self.service = service
        self.last_plan: Optional[RepairPlan] = None
        self.pre_state: Optional[Dict] = None  # 用于回滚

    async def execute_plan(self, plan: RepairPlan):
        self.logger.info(f"执行修复: {plan.description}")
        # 保存修复前状态（用于可能的回滚）
        self.pre_state = {
            "db_pool_size": self.service.db_pool_size,
            "service_up": self.service.service_up
        }
        try:
            if plan.action == "increase_pool":
                self.service.db_pool_size = plan.params.get("new_size", 20)
                self.logger.info(f"  连接池大小已调整为 {self.service.db_pool_size}")
            elif plan.action == "restart_service":
                # 模拟重启：设置 service_up=False 短暂时间
                self.service.service_up = False
                await asyncio.sleep(0.5)  # 模拟重启耗时
                self.service.service_up = True
                # 重启后连接池重置
                self.service.db_pool_size = self.service.normal_pool_size
                self.logger.info("  服务已重启，连接池重置")
            else:
                self.logger.warning(f"未知修复动作: {plan.action}")
        except Exception as e:
            self.logger.error(f"修复执行异常: {e}")
        # 等待一小段时间让系统稳定
        await asyncio.sleep(1)
        # 发起验证
        await self.send_message("verify_request", {
            "plan": plan,
            "pre_state": self.pre_state
        })

    async def run(self):
        while True:
            msg = await self.queue.get()
            if msg["type"] == "repair_plan":
                plan = msg["payload"]
                self.last_plan = plan
                await self.execute_plan(plan)
            elif msg["type"] == "rollback":
                # 执行回滚（恢复到修复前状态）
                if self.pre_state:
                    self.logger.warning("执行回滚...")
                    self.service.db_pool_size = self.pre_state["db_pool_size"]
                    self.service.service_up = self.pre_state["service_up"]
                    self.logger.info(f"已回滚到 db_pool_size={self.service.db_pool_size}, service_up={self.service.service_up}")
                else:
                    self.logger.error("无回滚状态，无法回滚")
                # 回滚后通知诊断Agent重新尝试
                await self.send_message("retry_diagnosis", "rollback_complete")
            else:
                pass


# -------------------- 7. VerificationAgent --------------------
class VerificationAgent(BaseAgent):
    def __init__(self, queue: asyncio.Queue, service: SimulatedService, threshold_ms=200):
        super().__init__("VerificationAgent", queue)
        self.service = service
        self.threshold_ms = threshold_ms
        self.check_count = 3  # 连续检查次数

    async def verify(self) -> VerificationResult:
        """多次采样验证延迟是否恢复正常"""
        total_lat = 0
        errors = 0
        samples = 5
        for _ in range(self.check_count):
            for _ in range(samples):
                lat = await self.service.handle_request()
                if lat == float('inf'):
                    errors += 1
                else:
                    total_lat += lat
            await asyncio.sleep(0.3)  # 间隔
        total_requests = self.check_count * samples
        if errors / total_requests > 0.5:
            return VerificationResult(False, f"错误率过高 ({errors}/{total_requests})")
        avg_lat = total_lat / (total_requests - errors) if (total_requests - errors) > 0 else float('inf')
        if avg_lat > self.threshold_ms:
            return VerificationResult(False, f"平均延迟 {avg_lat:.1f}ms 仍高于阈值 {self.threshold_ms}ms")
        return VerificationResult(True, f"延迟恢复正常: {avg_lat:.1f}ms")

    async def run(self):
        while True:
            msg = await self.queue.get()
            if msg["type"] == "verify_request":
                data = msg["payload"]
                plan = data["plan"]
                self.logger.info(f"开始验证修复: {plan.description}")
                result = await self.verify()
                self.logger.info(f"验证结果: {'成功' if result.success else '失败'} - {result.details}")
                if result.success:
                    self.logger.info("自愈成功，闭环完成。")
                    # 可以发送恢复消息清空监控告警状态
                    await self.send_message("healing_success", plan)
                else:
                    self.logger.warning("修复未生效，触发回滚并请求诊断Agent重新决策")
                    # 通知HealingAgent回滚
                    await self.send_message("rollback", plan)
            else:
                pass


# -------------------- 8. 系统编排 --------------------
async def main():
    # 创建共享消息队列和模拟服务
    queue = asyncio.Queue()
    service = SimulatedService()

    # 实例化Agent
    monitor = MonitorAgent(service, queue, threshold_ms=200)
    diagnosis = DiagnosisAgent(queue, service)
    healer = HealingAgent(queue, service)
    verifier = VerificationAgent(queue, service, threshold_ms=200)

    # 注入故障：故意将连接池设小，制造延迟
    logger.info("=== 注入故障：将数据库连接池缩小为2 ===")
    service.db_pool_size = 2

    # 启动所有Agent并行运行
    agents = [monitor, diagnosis, healer, verifier]
    tasks = [asyncio.create_task(agent.run()) for agent in agents]

    # 运行一段时间让自愈流程走完
    await asyncio.sleep(30)  # 30秒足够完成监控→诊断→修复→验证闭环

    # 收尾：取消所有任务
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("=== 演示结束 ===")

if __name__ == "__main__":
    asyncio.run(main())
