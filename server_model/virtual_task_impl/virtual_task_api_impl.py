import ujson
from abc import ABC

from conf import CONF
from db import a_redis as redis
from conf.flags import STOP_CODE
from utils import get_task_node_idx_log
from server_model.pod import Pod
from server_model.training_task_impl.additional_property_impl import \
    AdditionalPropertyImpl
from server_model.selector import AioTrainingTaskSelector, AioBaseTaskSelector


class VirtualTaskApiImpl(AdditionalPropertyImpl, ABC):
    async def log(self, rank: int, last_seen=None, **kwargs):  # 和TaskApiImpl的接口保持一致
        """
        获取任务链的日志，我们把之前的函数包装到这里
        @param rank:
        @return:
        """
        task = self.task
        child_tasks = await AioTrainingTaskSelector.where(None, '"chain_id" like %s', (f'{task.chain_id}_%',), limit=10000, order_desc=False)
        node = task.assigned_nodes[rank] if rank < len(task.assigned_nodes) else 'nan'
        # 获取error 日志
        try:
            error_msg = await AioBaseTaskSelector.get_error_info(id=task.id)
        except:
            error_msg = ""
        for child_task in child_tasks:
            pods = Pod.find_pods(int(child_task.id))
            for rank, pod in enumerate(pods):
                if pod.node == node:
                    res = await get_task_node_idx_log(str(child_task.id), task.user, rank, last_seen=last_seen, max_line_length=CONF.experiment.log.max_line_length)
                    try:
                        pod_id = f'{task.user.user_name}-{child_task.id}-{rank}'
                        exit_code = (await Pod.aio_find_pods_by_pod_id(pod_id))[0].exit_code
                    except:
                        exit_code = ""
                    return {
                        "data": res['data'],
                        "success": 1,
                        "msg": "get log successfully",
                        "last_seen": res['last_seen'],
                        "stop_code": child_task.stop_code,
                        "exit_code": exit_code,
                        "error_msg": error_msg
                    }
        return {
            "data": "",
            "success": 1,
            "msg": "get log successfully",
            "last_seen": None,
            "stop_code": 0,
            "exit_code": 0,
            "error_msg": error_msg
        }

    def select_pods(self):
        """
        获取虚拟任务的所有pods
        @return:
        """
        task = self.task
        pods = Pod.where("""
                "task_id" in (select "id" from "task_ng" where "chain_id" like %s)
                """, (f'{task.chain_id}_%',))
        node_pod_map = {
            pod.node: pod for pod in pods
        }
        task._pods_ = [node_pod_map.get(node, Pod.empty_pod(node=node)) for node in task.assigned_nodes]

    async def aio_select_pods(self):
        """
        获取虚拟任务的所有pods
        @return:
        """
        task = self.task
        pods = await Pod.a_where("""
        "task_id" in (select "id" from "task_ng" where "chain_id" like %s)
        """, (f'{task.chain_id}_%', ))
        node_pod_map = {
            pod.node: pod for pod in pods
        }
        task._pods_ = [node_pod_map.get(node, Pod.empty_pod(node=node)) for node in task.assigned_nodes]

    async def stop(self, *args, **kwargs):
        task_id_set = {pod.task_id for pod in self.task.pods} - {0}
        for task_id in task_id_set:
            await redis.lpush(
                f'{CONF.manager.stop_channel}:suspend:{task_id}',
                ujson.dumps({'stop_code': STOP_CODE.MANUAL_STOP})
            )

    async def suspend(self, restart_delay: int, *args, **kwargs):
        # virtual task 没有 suspend
        return await self.stop()
