

from .default import *
from .custom import *


import os.path
import re
from typing import TYPE_CHECKING, List

from base_model.base_task import BaseTask
from base_model.training_task import TrainingTask
from conf import CONF
from conf.flags import RunJobCode, BACKEND_UPGRADE, QUE_STATUS, TASK_TYPE, TASK_PRIORITY, TASK_OP_CODE
from db import a_redis as redis, MarsDB
from server_model.auto_task_impl import AutoTaskApiImpl
from server_model.task_impl import AioDbOperationImpl
from server_model.user import User
from utils import convert_to_external_task
from logm import logger
from api.task_schema import TaskSchema, TaskService


if TYPE_CHECKING:
    from server_model.user import User


async def operate_task_base(
        operate_user: str,
        task: TrainingTask,
        task_op_code=TASK_OP_CODE.STOP,
        restart_delay: int = 0,
        remote_apply: bool = False
):
    """

    @param operate_user:
    @param task:
    @param task_op_code: TASK_OP_CODE
    @param restart_delay
    @param remote_apply
    @return:
    """
    t = task
    job_info = t.job_info
    if t.queue_status == QUE_STATUS.FINISHED:
        return {
            'success': 1,
            'msg': f'{job_info} 已经停止，不再[{task_op_code.value}]',
        }

    # 只能关闭队列中的 upgraded 任务
    if t.backend == BACKEND_UPGRADE:
        return {
            'success': 0,
            'msg': f'{t.job_info}: 执行停止出错, 不能操作升级任务'
        }

    if t.queue_status == QUE_STATUS.QUEUED:
        if task_op_code == TASK_OP_CODE.SUSPEND:
            return {
                'success': 1,
                'msg': f'{job_info} 已经 [{QUE_STATUS.QUEUED}]，不再[{task_op_code.value}]',
            }
        # STOP
        # queue 的任务，需要标记成 finished
        await t.re_impl(AioDbOperationImpl).update(fields=('queue_status', ),  values=(QUE_STATUS.FINISHED, ), remote_apply=remote_apply)
        return {
            'success': 1,
            'msg': f'{job_info} 并没有在运行',
        }

    # QUE_STATUS.SCHEDULED:
    try:
        t.re_impl(AutoTaskApiImpl)
        if task_op_code in {TASK_OP_CODE.STOP, TASK_OP_CODE.FAIL, TASK_OP_CODE.SUCCEED}:
            msg = await t.stop(task_op_code=task_op_code)
        else:
            if task.user_name != operate_user:
                msg = f'{operate_user} 试图打断 {task.user_name} 的任务 [{task.nb_name}][{task.id}]'
                logger.warning(msg)
                return {'success': 0, 'msg': '无权操作他人的任务'}
            msg = await t.suspend(restart_delay)
    except Exception as e:
        msg = f'{job_info} 执行[{task_op_code.value}]时出现错误，请联系系统组: {e}'
        return {
            'success': 0,
            'msg': msg,
        }

    return {
        'success': 1,
        'msg': f'{job_info} 已对 id {t.id} 执行 [{task_op_code.value}] 命令，原来状态{t.chain_status}，返回信息 {msg}',
    }


async def check_restart(lt):
    stop_code = 0
    if await redis.get(f'ban:{lt.user_name}:{lt.nb_name}:{lt.chain_id}'):
        return False
    recorded_stop_code = await redis.get(f'lifecycle:{lt.id}:stop_code')
    if not recorded_stop_code:
        return False
    for code in recorded_stop_code.decode().strip().split('\n'):
        stop_code |= int(code)
    return 1 < stop_code < 64 or stop_code >= 2048


def get_service_config_error(service: TaskService, services: List[TaskService]):
    # name check
    if sum([svc.name == service.name for svc in services]) > 1:
        return f'service name 有重复：{service.name}'
    if re.fullmatch('[a-z0-9]([-a-z0-9]*[a-z0-9])?', service.name) is None:
        return f"{service.name} 服务的命名不合法：只能包含小写字母、数字，以及 '-'；必须以字母数字开头和结尾"

    # port check
    if service.type != 'local':
        if service.rank != [0]:
            return f'{service.name} 服务类型不为 local, 仅支持在 rank=0 节点启动' # 涉及 nodeport 和 ingress, 不能多节点
        if service.port is None:
            return f'{service.name} 服务类型不为 local，必须指定端口'
        if service.port not in range(1, 65536):
            return f'{service.name} 服务的端口不合法：需要在 [1, 65535] 范围内'
        for existing_svc in services:
            if existing_svc != service and service.port == existing_svc.port:
                return f'{service.name} 服务设定的端口 {service.port} 与 {existing_svc.name} 服务的端口冲突'
    elif service.port is not None:
        return f'{service.name} 服务类型为 local，不能指定端口'

    # startup_script check
    if service.startup_script and any(token in service.startup_script for token in [';', '\n']):
        return f'启动命令仅支持单条 bash 命令，复杂指令请编写 bash 脚本运行。({service.name} 服务启动命令包含 ";" 或 "\\n")'

    # type check
    if service.type not in ['http', 'tcp', 'local']:
        return f'不支持的服务类型：{service.type}'
    return None


def check_services_config_get_err(services, user):
    # 为内建服务设置端口, 以供后续检验端口是否有重复
    for service in filter(lambda svc: svc.name in CONF.jupyter.builtin_services, services):
        if any(x is not None for x in [service.port, service.type, service.startup_script]):
            return f'"{service.name}" 是系统保留的内建服务名, 自定义服务请不要以此命名'
        service.port = CONF.jupyter.builtin_services.get(service.name).get('port', None)
    # 检查自定义服务的参数
    for service in filter(lambda svc: svc.name not in CONF.jupyter.builtin_services, services):
        if not user.is_internal: # TODO(role): 自定义服务
            return '无权使用自定义服务'
        if (err_msg := get_service_config_error(service, services)) is not None:
            return err_msg
    return None


async def create_base_task(task: BaseTask, tags: list = [], remote_apply: bool = False):
    try:
        task = await task.re_impl(AioDbOperationImpl).create(remote_apply=remote_apply)
        async with MarsDB() as conn:
            for tag in tags:
                await task.tag_task(tag, a_db_conn=conn)
    except Exception as e:
        if "already exists" in str(e):
            return {
                'success': RunJobCode.EXISTS.value,
                'msg': f'您名字为 {task.nb_name} 的任务正在运行，不能重复创建，请稍后重试'
            }
        else:
            logger.exception(e)
            return {
                'success': RunJobCode.FATAL.value,
                'msg': '未能在数据库中成功创建队列，请联系系统组',
            }
    task: TrainingTask = convert_to_external_task(task)
    return {
        'success': RunJobCode.QUEUED.value,
        'msg': '任务创建队列成功，请等待调度',
        'task': task.trait_dict()
    }


async def create_task_base_queue_v2(user: User, task_schema: TaskSchema = None, raw_task_schema: dict = None, remote_apply: bool = False):
    """

    :param task_schema:
    :param user:
    :param raw_task_schema: 原始的任务 schema 为 json 的 string
    :param remote_apply: 创建任务是否要等到从库都返回了才成功
    :return: dict(success, msg)
    """
    if raw_task_schema is None:
        raw_task_schema = task_schema.dict()
    if task_schema is None:
        task_schema = TaskSchema.parse_obj(raw_task_schema)
    raw_task_schema.pop('token', None)

    fatal_response = lambda msg: {'success': RunJobCode.FATAL.value, 'msg': msg}
    # 基础校验
    unsupported_chars = ['(', ')']
    if any(illegal_chars := list(c for c in unsupported_chars if c in task_schema.name)):
        return fatal_response(f'name 包含不支持的命名字符：{illegal_chars}')
    if task_schema.version != 2:
        return fatal_response('task config 版本不对，应该是 [2]')
    if task_schema.task_type not in TASK_TYPE.all_task_types():
        return fatal_response('无效的task_type')
    if task_schema.spec is None:
        return fatal_response('必须指定 task spec')
    if len(task_schema.spec.workspace) > 255:
        return fatal_response('workspace 长度不应超过 255')
    if ' ' in task_schema.spec.workspace:
        return fatal_response('workspace 不允许有空格')
    if task_schema.resource.node_count <= 0:
        return fatal_response('节点数必须大于 0')
    # 组装成数据库需要的 code file
    code_file = os.path.join(task_schema.spec.workspace, task_schema.spec.entrypoint) + ' ' + task_schema.spec.parameters
    if len(code_file) > 2047:
        return fatal_response('运行命令 [workspace + entrypoint + params] 长度不应超过 2047')
    # 不按照分组判断，以免有人换分组名字无限提交
    MAX_TASKS = 10000
    ts_count = (await MarsDB().a_execute("""
        select count(*) from "unfinished_task_ng"
        where "user_name" = %s
    """, (user.user_name, ))).fetchall()[0][0]
    if ts_count >= MAX_TASKS:
        return fatal_response(f'您提交的[未运行完成任务]已经超过了 [{MAX_TASKS}] 个')

    # 获取 group
    group = task_schema.resource.group
    if (not task_schema.resource.group) or task_schema.resource.group.lower() == 'default':
        group = CONF.scheduler.default_group
    client_group = group
    schedule_zone = None
    if '#' in group:
        group, schedule_zone = group.split('#')

    # schedule_zone 相关
    override_node_resource = task_schema.options.get('override_node_resource', None)

    # 用户相关
    if user.is_external:
        task_schema.priority = TASK_PRIORITY.AUTO.value

    # 对 image 进行处理, template 表示系统内建镜像；train_image 表示用户自定义镜像
    if task_schema.resource.image is not None and '/' in task_schema.resource.image:
        template = 'train_image:' + task_schema.resource.image.split('/')[-1]
        train_image = task_schema.resource.image
    else:
        template = task_schema.resource.image or 'default'
        train_image = None
    if template in ['default', 'DEFAULT']:
        user_train_envs = user.quota.train_environments
        if len(user_train_envs) == 0:
            return fatal_response('至少要有一个可用的 train_environments')
        template = user_train_envs[0]
    if (err_msg := await check_environment_get_err(train_image, template, user)) is not None:
        return fatal_response(err_msg)

    # service 配置校验
    if (err_msg := check_services_config_get_err(task_schema.services, user)) is not None:
        return fatal_response(err_msg)

    # 对 train task 的处理
    # if task_schema.task_type == TASK_TYPE.TRAINING_TASK:
    #     # 只需要对 training 任务判断 quota
    #     available_priority = user.quota.available_priority(group)
    #     if task_schema.priority not in available_priority.values():
    #         if len(available_priority.values()) > 0:
    #             task_schema.priority = min(available_priority.values())
    #         else:
    #             msg = ' , '.join(
    #                 f'{p_value} ({p_name})'
    #                 for p_name, p_value in
    #                 sorted(available_priority.items(), key=lambda p: p[1])
    #             )
    #             return {
    #                 'success': RunJobCode.FATAL.value,
    #                 'msg': f'无效的优先级，可选为：{msg}'
    #             }

    # 先构造一个基础的 task
    task = BaseTask(
        implement_cls=AioDbOperationImpl, id=None, nb_name=task_schema.name,
        user_name=user.user_name, code_file=code_file,
        workspace=task_schema.spec.workspace, group=group, nodes=task_schema.resource.node_count,
        restart_count=0, # delete me
        backend=template,
        queue_status=QUE_STATUS.QUEUED, priority=task_schema.priority,
        chain_id=None,   # delete me
        task_type=task_schema.task_type,
        whole_life_state=task_schema.options.get('whole_life_state', 0),
        mount_code=task_schema.options.get('mount_code', 2),
        # options 包容万物
        assigned_nodes=task_schema.options.get('assigned_nodes', []),
    )
    task.user = user
    task.config_json = {
        'client_group': client_group,
        'whole_life_state': task_schema.options.get('whole_life_state', 0),
        'environments': task_schema.spec.environments,
        'schedule_zone': schedule_zone,
        'train_image': train_image,  # 用于 client 端获取镜像完整 URL
        'override_node_resource': override_node_resource,
        'schema': raw_task_schema,  # 保存提交时候的样子
    }
    result = await process_create_task(task_schema=task_schema, task=task)
    if result.get('success', 0) != 1:
        return result
    task = result['task']
    tags = task_schema.options.get('tags', [])
    if isinstance(tags, str):
        tags = tags.split(',')
    tags = [str(t) for t in tags if t] if isinstance(tags, list) else []
    return await create_base_task(task, tags=tags, remote_apply=remote_apply)
