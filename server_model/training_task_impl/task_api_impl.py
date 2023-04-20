import json
from abc import ABC

from conf import CONF
from conf.flags import STOP_CODE, CHAIN_STATUS, TASK_OP_CODE
from db import a_redis as redis, MarsDB
from utils import get_task_node_idx_log
from server_model.pod import Pod
from server_model.training_task_impl.additional_property_impl import \
    AdditionalPropertyImpl
from server_model.selector import AioBaseTaskSelector


class TaskApiImpl(AdditionalPropertyImpl, ABC):
    async def log(self, rank: int, last_seen=None, service=None, **kwargs):
        """
        获取任务链的日志，我们把之前的函数包装到这里
        @param rank:
        @param last_seen:
        @param service: 指定的服务名，值为 None 时查看所有日志
        @return:
        """
        task = self.task
        data_list = []
        error_msg = ''
        exit_code = ''
        stop_code = 0
        rst_last_seen = None
        last_seen_id = 0 if last_seen is None else last_seen.get('id', 0)
        current_seen_id = last_seen_id
        if service is not None:
            suffix_filter = f'{service}.service_log'
            task_id_list = [task.id]    # 服务日志只查询当前任务, 不查询整个 chain
        else:
            suffix_filter = f'#{rank}'
            task_id_list = sorted(task.id_list)
        for task_id in task_id_list:
            if task_id < last_seen_id:
                continue
            res = await get_task_node_idx_log(task_id, task.user, rank, last_seen=last_seen, suffix_filter=suffix_filter, max_line_length=CONF.experiment.log.max_line_length)
            if res['data'] != "还没产生日志":
                data_list.append(res['data'])
                current_seen_id = max(current_seen_id, task_id)
            if res['last_seen'] and res['last_seen']['timestamp']:
                if not rst_last_seen or res['last_seen']['timestamp'] > rst_last_seen['timestamp']:
                    rst_last_seen = res['last_seen']
            if task_id == task_id_list[-1]:
                try:
                    error_msg = await AioBaseTaskSelector.get_error_info(id=task_id)
                except:
                    error_msg = ""
                try:
                    pod_id = f'{task.user.user_name}-{task_id}-{rank}'
                    exit_code = (await Pod.aio_find_pods_by_pod_id(pod_id))[0].exit_code
                except:
                    exit_code = ""
                stop_code = (await AioBaseTaskSelector.find_one(None, id=task_id)).stop_code
        if rst_last_seen is not None:
            rst_last_seen['id'] = current_seen_id
        return {
            "stop_code": stop_code,
            "exit_code": exit_code,
            "error_msg": error_msg,
            "data": "\n".join([data for data in data_list if data]) if data_list else "还没产生日志",
            "restart_log": await self.restart_log(),
            "success": 1,
            "msg": "get log successfully",
            "last_seen": rst_last_seen
        }

    async def sys_log(self):
        """
        获取任务链的系统报错日志
        :return:
        """
        task = self.task
        task_id_list = sorted(task.id_list)
        data = '=' * 20 + '\n'
        for task_id in task_id_list:
            shown_id = f"{task_id} (current)" if task_id == task_id_list[-1] else f"{task_id}"
            data += f"id: {shown_id}\n{'-' * 10}\nsyslog:\n"
            try:
                sys_msg = await AioBaseTaskSelector.get_error_info(id=task_id)
                data += f"{sys_msg.strip()}\n"
                for rank in range(len(task.assigned_nodes)):
                    res = await get_task_node_idx_log(task_id, task.user, rank, suffix_filter='.oom_log', max_line_length=CONF.experiment.log.max_line_length)
                    if res['data'] != "还没产生日志" and res['data'] != "":
                        data += f"oom log:\n{res['data']}"
                data += f"\n{'=' * 20}\n"
            except:
                data += f"\n{'=' * 20}\n"
        return {
            "success": 1,
            "data": data
        }

    async def restart_log(self):
        try:
            task = self.task
            restart_log = await MarsDB().a_execute(f"""
            select
                "task_id",
                json_agg(json_build_object(
                    'rule', "rule",
                    'reason', "reason",
                    'result', "result"
                )) as "restart_log"
            from "task_restart_log"
            where "task_id" in ({','.join(str(t) for t in task.id_list)})
            group by "task_id"
            """)
            return {
                r.task_id: r.restart_log for r in restart_log
            }
        except Exception as e:
            print(e)
            return {}

    async def search_in_global(self, content):
        """
        全局搜索该任务每个rank包含content的次数
        :return:
        """
        task = self.task
        task_id_list = sorted(task.id_list)
        rst = [0] * len(task.assigned_nodes)
        for task_id in task_id_list:  # 所有任务
            for rank in range(len(task.assigned_nodes)):  # 所有节点
                res = await get_task_node_idx_log(task_id, task.user, rank, max_line_length=CONF.experiment.log.max_line_length)
                if res['data'] != "还没产生日志":
                    for line in res['data'].split('\n'):  # 所有行
                        rst[rank] += line[29:].count(content)
        return {
            'success': 1,
            'data': rst
        }

    async def stop(self, task_op_code=TASK_OP_CODE.STOP, *args, **kwargs):
        """
        停止这个任务
        @return:
        """
        task = self.task
        await redis.set(f'ban:{task.user_name}:{task.nb_name}:{task.chain_id}', 1)  # 防止重启
        # graceful stop
        if task_op_code == TASK_OP_CODE.FAIL:
            stop_code = STOP_CODE.MANUAL_FAILED
        elif task_op_code == TASK_OP_CODE.SUCCEED:
            stop_code = STOP_CODE.MANUAL_SUCCEEDED
        else:
            stop_code = STOP_CODE.MANUAL_STOP
        await redis.lpush(f'{CONF.manager.stop_channel}:suspend:{task.id}', json.dumps({'stop_code': stop_code}))

    async def suspend(self, restart_delay: int, **kwargs):
        """
        打断这个任务
        @param restart_delay: 这个任务打断之后等待多少秒加入队列
        @return:
        """
        task = self.task
        await redis.lpush(
            f'{CONF.manager.stop_channel}:suspend:{task.id}',
            json.dumps({'stop_code': STOP_CODE.INTERRUPT}))

    def select_pods(self):
        """
        处于挂起的任务，用之前任务的 pod，这样也能拿到日志

        @param: no_chain
        @return:
        """
        task = self.task
        id_list = sorted(task.id_list)
        chain_status = task.chain_status
        if chain_status == CHAIN_STATUS.WAITING_INIT:
            task._pods_ = []
        elif chain_status == CHAIN_STATUS.SUSPENDED:
            task._pods_ = Pod.find_pods(id_list[-2])
        else:  # finished
            task._pods_ = Pod.find_pods(id_list[-1])
            # suspend 之后在排队状态被 stop 了，应该拿上一个
            if len(id_list) > 1 and len(task._pods_) == 0:
                task._pods_ = Pod.find_pods(id_list[-2])

    async def aio_select_pods(self):
        """
        处于挂起的任务，用之前任务的 pod，这样也能拿到日志

        @param: no_chain
        @return:
        """
        task = self.task
        id_list = sorted(task.id_list)
        chain_status = task.chain_status
        if chain_status == CHAIN_STATUS.WAITING_INIT:
            task._pods_ = []
        elif chain_status == CHAIN_STATUS.SUSPENDED:
            task._pods_ = await Pod.aio_find_pods(id_list[-2])
        else:  # finished
            task._pods_ = await Pod.aio_find_pods(id_list[-1])
            # suspend 之后在排队状态被 stop 了，应该拿上一个
            if len(id_list) > 1 and len(task._pods_) == 0:
                task._pods_ = await Pod.aio_find_pods(id_list[-2])
