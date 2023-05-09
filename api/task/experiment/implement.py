
from .default import *
from .custom import *

import ciso8601
import datetime
import inspect
import json
import pickle
import time
import urllib
from typing import Optional, List
from fastapi import Depends, Request, Query, HTTPException

from logm import logger
from api.depends import get_api_user_with_token, get_api_task, JUPYTER_ADMIN_GROUP, check_user_access_to_task
from api.task_schema import TaskSchema
from api.task.service_task import create_service_task, VISIBLE_TASK_TAG
from api.operation import operate_task_base, create_task_base_queue_v2
from base_model.training_task import TrainingTask
from conf.flags import TASK_OP_CODE, TASK_PRIORITY, QUE_STATUS, STOP_CODE, EXP_STATUS, TASK_TYPE
from db import MarsDB
from db import a_redis as redis
from server_model.auto_task_impl import AutoTaskApiImpl
from server_model.selector import AioUserSelector
from server_model.task_impl import AioDbOperationImpl
from server_model.training_task_impl import TaskApiImpl, DashboardApiImpl
from server_model.user import User
from server_model.task_runtime_config import TaskRuntimeConfig
from utils import convert_to_external_node, convert_to_external_task, get_task_node_idx_log


async def create_task_v2(
        request: Request,
        task_schema: TaskSchema,
        user_name: str = None,
        api_user: User = Depends(get_api_user_with_token()),
    ):
        if user_name is not None and api_user.user_name != user_name:     # 为其他人创建任务
            # 目前只允许 hub admin 为他人创建开发容器
            if task_schema.task_type != TASK_TYPE.JUPYTER_TASK or not api_user.in_group(JUPYTER_ADMIN_GROUP):
                raise HTTPException(403, detail='无权为他人创建任务')
            user = await AioUserSelector.find_one(user_name=user_name)
            if user is None:
                raise HTTPException(404, detail=f'用户 [{user_name}] 不存在')
        else:
            user = api_user

        if task_schema.task_type == TASK_TYPE.JUPYTER_TASK:
            return await create_service_task(user=user, task_schema=task_schema, raw_task_schema=await request.json())
        else:
            return await create_task_base_queue_v2(user=user, task_schema=task_schema, raw_task_schema=await request.json(), remote_apply=True)


async def resume_task(
    task: TrainingTask = Depends(get_api_task()),
    user: User = Depends(get_api_user_with_token())
):
    task.re_impl(AioDbOperationImpl)
    try:
        await redis.delete(f'ban:{task.user_name}:{task.nb_name}:{task.chain_id}')
        task = await task.resume(remote_apply=True)
    except Exception as e:
        if "already exists" in str(e):
            return {
                'success': 0,
                'msg': f'您名字为 {task.nb_name} 的任务正在运行，不能重复创建，请稍后重试'
            }
        else:
            logger.exception(e)
            return {
                'success': 0,
                'msg': '未能在数据库中成功创建队列，请联系系统组',
            }
    task: TrainingTask = convert_to_external_task(task)
    return {
        'success': 1,
        'msg': '直接插入队列成功，请等待调度',
        'task': task.trait_dict()
    }


async def stop_task(op: TASK_OP_CODE = TASK_OP_CODE.STOP, task: TrainingTask = Depends(get_api_task())):
    await redis.set(f'ban:{task.user_name}:{task.nb_name}:{task.chain_id}', 1)
    res = await operate_task_base(operate_user=task.user_name, task=task, task_op_code=op, remote_apply=False)
    return res


async def suspend_task_by_name(
        task: TrainingTask = Depends(get_api_task()),
        restart_delay: int = 0,
        version: str = "new",   # 兼容 jupyter, 前端更新后删除
):
    res = await operate_task_base(operate_user=task.user_name, task=task, task_op_code=TASK_OP_CODE.SUSPEND, restart_delay=restart_delay, remote_apply=False)
    return res


async def task_node_log_api(task: TrainingTask = Depends(get_api_task()), rank: int = 0, last_seen: str = 'null', service: str = None):
    try:
        last_seen = json.loads(last_seen)
    except:
        last_seen = None
    if last_seen:
        try:
            last_seen['timestamp'] = datetime.datetime.strptime(last_seen['timestamp'], "%Y-%m-%dT%H:%M:%S.%f")
        except:
            last_seen['timestamp'] = datetime.datetime.strptime(last_seen['timestamp'], "%Y-%m-%dT%H:%M:%S")
    task.re_impl(AutoTaskApiImpl)
    res = await task.log(rank, last_seen=last_seen, service=service)
    # 兜底逻辑，任务没启动就失败了，日志文件都没有，标记 stop
    if task.queue_status == QUE_STATUS.FINISHED and res['stop_code'] == STOP_CODE.NO_STOP and res['data'] == '还没产生日志':
        res['stop_code'] = STOP_CODE.STOP
    return res


async def task_sys_log_api(task: TrainingTask = Depends(get_api_task())):
    res = await task.re_impl(TaskApiImpl).sys_log()
    # if not user.is_internal:
    #     res['data'] = ''  # 系统错误日志里含节点信息，先不给外部用户看
    return res


async def task_search_in_global(content, task: TrainingTask = Depends(get_api_task()), user=Depends(get_api_user_with_token())):
    content = urllib.parse.unquote(content)
    res = await task.re_impl(TaskApiImpl).search_in_global(content)
    return res


async def chain_perf_series_api(task: TrainingTask = Depends(get_api_task()),
                                user: User = Depends(get_api_user_with_token()), typ: str = 'gpu', rank: int = 0, data_interval: Optional[str]= '5min'):
    # data_interval 使用query参数传入
    if data_interval not in ('1min', '5min'):
        data_interval = '5min'
    try:
        data = await task.re_impl(DashboardApiImpl).get_chain_time_series(typ, rank, data_interval=data_interval)
        if not user.is_internal:
            for item in data:
                if 'node' in item:
                    item['node'] = convert_to_external_node(item['node'], 'rank', item['rank'])
        return {
                'success': 1,
                'data': data
        }
    except ValueError as e:
        return{
            'success':0,
            'msg': str(e)
        }


INTERNAL_TAGS = {VISIBLE_TASK_TAG}


async def tag_task(tag: str, task: TrainingTask = Depends(get_api_task())):
    if tag in INTERNAL_TAGS:
        return {'success': 0, 'msg': f'[{tag}] 是内部保留 tag, 请使用其他命名'}
    await task.re_impl(AioDbOperationImpl).tag_task(tag, remote_apply=True)
    return {
        'success': 1,
        'msg': f'训练任务[{task.job_info}] 设置 tag {tag} 标记成功'
    }


async def untag_task(tag: str, task: TrainingTask = Depends(get_api_task())):
    if tag in INTERNAL_TAGS:
        return {'success': 0, 'msg': f'[{tag}] 是内部保留 tag, 无法删除'}
    await task.re_impl(AioDbOperationImpl).untag_task(tag, remote_apply=True)
    return {
        'success': 1,
        'msg': f'训练任务[{task.job_info}] 取消 tag {tag} 标记成功'
    }


async def delete_tags(tag: List[str] = Query(default=None), user: User = Depends(get_api_user_with_token())):
    if tag is None:
        return {
            'success': 0,
            'msg': '请指定要删除的 tag'
        }
    if any(t in INTERNAL_TAGS for t in tag):
        return {'success': 0, 'msg': f'不能删除内部保留的 tag: {[t for t in tag if t in INTERNAL_TAGS]}'}
    await MarsDB().a_execute(f"""
    delete from "task_tag" where "user_name" = '{user.user_name}' and tag in ('{"','".join(tag)}')
    """)
    return {
        'success': 1,
        'msg': f'成功删除 tag {tag}'
    }


async def get_task_tags(user: User = Depends(get_api_user_with_token())):
    tags = [
        r.tag for r in
        await MarsDB().a_execute(f"""select distinct "tag" from "task_tag" where user_name = '{user.user_name}' """)
    ]
    tags = list(set(tags) - set(INTERNAL_TAGS))
    return {
        'success': 1,
        'result': tags
    }


async def a_is_api_limited(key=None, waiting_seconds: int = 10) -> bool:
    """
    api限流，本次操作后waiting_seconds秒内再次操作会返回True（被限制），否则返回False（未被限制）
    :param key:
    :param waiting_seconds: 单位为秒
    :return:
    """
    try:
        key = f'{inspect.getframeinfo(inspect.currentframe().f_back)[2]}{key}'
        exist_key = await redis.exists(key)
        if exist_key:
            return True
        else:
            await redis.set(key, waiting_seconds)
            await redis.expire(key, waiting_seconds)
            return False
    except:
        return False


async def update_priority(
        priority: int = None,
        custom_rank: float = None,
        t: TrainingTask = Depends(get_api_task()),
        user: User = Depends(get_api_user_with_token()),
):
    if t.queue_status == QUE_STATUS.FINISHED:
        return {
            'success': 0,
            'msg': f'不能更新已经结束任务的优先级'
        }
    if priority is None and custom_rank is None:
        return {
            'success': 0,
            'msg': f'必须指定要更新的字段'
        }
    # 限流的时间
    waiting_seconds = 10
    if await a_is_api_limited(key=t.id, waiting_seconds=waiting_seconds):
        return {
            'success': 0,
            'msg': f'该任务在{waiting_seconds}秒内已经更新过优先级'
        }
    if priority:
        if not user.is_internal:
            priority = -1
        try:
            priority = int(priority)
            if priority not in [
                TASK_PRIORITY.EXTREME_HIGH.value, TASK_PRIORITY.VERY_HIGH.value, TASK_PRIORITY.HIGH.value,
                TASK_PRIORITY.ABOVE_NORMAL.value, TASK_PRIORITY.AUTO.value
            ]:
                raise Exception()
        except:
            return {'success': 0, 'msg': '优先级设置不对，请参考 hfai.client.EXP_PRIORITY'}
    if priority is not None:
        await t.re_impl(AioDbOperationImpl).update(('priority', ), (priority, ), remote_apply=False)
    runtime_config_json = {
        'update_priority_called': True
    }
    if custom_rank is not None:
        runtime_config_json['custom_rank'] = custom_rank
    await TaskRuntimeConfig(t).a_insert('runtime_priority', runtime_config_json, chain=True, update=True)
    return {
        'success': 1,
        'msg': f'成功修改 [{t.user_name}][{t.nb_name}] 的{" priority 为 " + str(priority) if priority is not None else ""}'
               f'{" custom_rank 为 " + str(custom_rank) if custom_rank is not None else ""}',
        'timestamp': time.time()
    }


async def switch_group(group: str = None, task: TrainingTask = Depends(get_api_task())):
    """
    修改任务的分组， 会在返回的 data 中提供 task 的 chain_id
    :param group:
    :param task:
    :return:
    """
    await MarsDB().a_execute("""
    update "task_ng" set "group" = %s where "chain_id" = %s
    """, (group, task.chain_id))
    return {
        'success': 1,
        'data': {
            'chain_id': task.chain_id
        }
    }


async def get_task_on_node_api(node: str, tick : datetime.datetime, with_log : bool = False, log_context_in_sec=60,
                               user: User = Depends(get_api_user_with_token(allowed_groups=['ops', 'system', 'platform']))):
    """
    根据时间戳查询当时在节点上运行的训练任务, 并返回指定时间前后的任务日志
    """
    sql = f'''
        select
            "task_ng"."id", "task_ng"."user_name", "task_ng"."nb_name", "task_ng"."begin_at", "task_ng"."end_at",
            "task_ng"."assigned_nodes", "task_ng"."group"
        from "pod_ng"
        inner join "task_ng" on "task_ng"."id" = "pod_ng"."task_id"
        where
            "task_ng"."task_type" = %s and
            "pod_ng"."node" = %s and
            "pod_ng"."begin_at" < %s and
            ("pod_ng"."end_at" > %s or "pod_ng"."status" not in (%s, %s, %s))
    '''
    res = await MarsDB().a_execute(sql, (TASK_TYPE.TRAINING_TASK, node, tick, tick, *EXP_STATUS.FINISHED))
    if with_log:
        task_logs = []
        for task in res:
            user = await AioUserSelector.find_one(user_name=task.user_name)
            node_rank = [rank for rank, rank_node in enumerate(task.assigned_nodes) if rank_node == node][0]
            node_log = await get_task_node_idx_log(task.id, user, node_idx=node_rank)
            task_log = ''
            for line in node_log['data'].splitlines():
                try:
                    ts = ciso8601.parse_datetime(line[1:27])
                except:
                    continue
                if abs((ts - tick).total_seconds()) <= log_context_in_sec:
                    task_log += line + '\n'
            task_logs.append(task_log)
        additional_response = {'logs': task_logs}
    else:
        additional_response = {}

    return {
        'success': 1,
        # 目前只查训练任务正常情况下最多只有一个任务, 这里先以 list 形式返回, 方便之后有需求的话支持 background task / jupyter 之类的
        'result': [{**r} for r in res],
        **additional_response,
    }


async def service_control_api(service: str, action: str, task: TrainingTask = Depends(get_api_task())):
    task.re_impl(AutoTaskApiImpl)
    if task.runtime_config_json.get('service_task', {}).get('version', 0) < 1:
        return {
            'success': 0,
            'msg': '容器版本较老, 不支持此功能, 请重启容器后重试'
        }
    if action not in ['start', 'stop', 'restart']:
        raise HTTPException(400, detail=f'不支持的操作: {action}')
    msg = {'service': service, 'action': action}
    await redis.lpush(f'manager_service_control:{task.id}', pickle.dumps(msg))
    return {
        'success': 1,
        'msg': '已成功发送信号, 检查服务日志以查看操作结果'
    }


async def set_task_restart_log_api(rule: str, reason: str, result: str, task: TrainingTask = Depends(get_api_task(chain_task=False))):
    task.re_impl(AioDbOperationImpl)
    return await task.set_restart_log(rule, reason, result)
