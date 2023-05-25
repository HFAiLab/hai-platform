

from .default import *
from .custom import *

from typing import Dict
from fastapi import Depends
from munch import Munch

from api.depends import get_api_task, get_api_user_with_token
from api.task_schema import TaskSchema
from api.operation import check_restart, check_environment_get_err, check_services_config_get_err
from api.utils import failed_response
from base_model.base_task import BaseTask
from conf import MARS_GROUP_FLAG, CONF
from conf.flags import QUE_STATUS, TASK_TYPE, TASK_PRIORITY, USER_ROLE
from db import MarsDB
from k8s.async_v1_api import async_get_nodes_df, async_set_node_label
from server_model.selector import AioBaseTaskSelector, AioTrainingTaskSelector
from server_model.task_impl import AioDbOperationImpl
from server_model.user import User

QUOTA_SPOT_JUPYTER = 'spot_jupyter'
QUOTA_DEDICATED_JUPYTER = 'dedicated_jupyter'
VISIBLE_TASK_TAG = '_visible_jupyter'
SHARED_NODE_GROUP = CONF.jupyter.shared_node_group_prefix


async def get_spot_jupyter_status():
    nodes_df = await async_get_nodes_df()
    free_nodes = nodes_df[
        (nodes_df.status == 'Ready') &
        (nodes_df.group == CONF.scheduler.default_group) &
        (nodes_df.working.isnull() | (nodes_df.working_user_role == USER_ROLE.EXTERNAL)) # 外部用户训练节点也视为空闲节点
    ]
    return Munch(
        too_busy_to_create = (busy:= len(free_nodes) < CONF.jupyter.spot.num_free_node_thresholds.min_to_create),
        max_num_exceeded = (exceeded := nodes_df.mars_group.str.contains('external_spot').astype(bool).sum().item() >= CONF.jupyter.spot.max_number),
        can_create = not busy and not exceeded,
        can_run = len(free_nodes) >= CONF.jupyter.spot.num_free_node_thresholds.min_to_run,
    )


async def check_external_dedicated_jupyter(user, task_schema: TaskSchema):
    is_spot = task_schema.resource.is_spot
    quota = int(user.quota.quota(QUOTA_SPOT_JUPYTER if is_spot else QUOTA_DEDICATED_JUPYTER))
    type_name = 'Spot 独占开发容器' if is_spot else '独占开发容器'

    # quota 和 任务数量检验
    if int(user.quota.quota(f'jupyter:{task_schema.resource.group}')) == 0:
        return False, f"用户不能在 {task_schema.resource.group} 上创建 Jupyter, 请联系管理员" # 实际上从前端正常提交任务不会走到这里
    if quota == 0:
        return False, f"用户无创建 [{type_name}] 的权限, 请联系管理员"
    sql = '''
        select "nb_name" from "unfinished_task_ng" where
            "user_name" = %s and "queue_status" != %s and "group" not like %s and 
            coalesce(("config_json"->'schema'->'resource'->'is_spot')::bool, false) = %s
    '''
    tasks = (await MarsDB().a_execute(sql, (user.user_name, QUE_STATUS.FINISHED, SHARED_NODE_GROUP+'%', is_spot))).fetchall()
    if len(tasks) >= quota:
        return False, f"最多创建 {quota} 个{type_name}, 已创建 {len(tasks)} 个 ({','.join(t.nb_name for t in tasks)})"

    # 创建 Spot jupyter 需要检验集群状态
    if is_spot:
        # 虽然 nodes_df 有延迟, 但 scheduler 有二次检验, 不满足条件的任务即使交上去也会被调度模块关掉
        spot_status = await get_spot_jupyter_status()
        if spot_status.too_busy_to_create:
            return False, "集群当前较忙, 不允许创建 Spot 独占开发容器"
        if spot_status.max_num_exceeded:
            return False, "集群当前 Spot 独占开发容器总数超限"
    return True, "允许创建"


async def clear_visible_tag(nb_name: str, user_name: str):
    sql = f'''
        delete from "task_tag" where "chain_id" in (
            select "chain_id" from "task_ng"
            where "nb_name" = %s and "task_ng"."user_name" = %s and "task_type" = '{TASK_TYPE.JUPYTER_TASK}'
                and "task_ng"."chain_id" in (   -- task_ng 表较大, 进一步过滤加快速度
                    select "chain_id" from "task_tag" where "user_name" = %s and "tag" = '{VISIBLE_TASK_TAG}'
                )
        ) and "tag" = '{VISIBLE_TASK_TAG}'
    '''
    await MarsDB().a_execute(sql, params=(nb_name, user_name, user_name))


async def create_service_task(
        user: User,
        task_schema: TaskSchema,
        raw_task_schema: Dict,
    ):
        if task_schema.task_type != TASK_TYPE.JUPYTER_TASK:
            return failed_response('该接口仅能创建 jupyter task')
        if task_schema.resource.node_count != 1:
            return failed_response(f'开发容器仅支持单节点')
        lt = await AioBaseTaskSelector.find_one(None, nb_name=task_schema.name, user_name=user.user_name)
        if lt is not None and lt.queue_status != QUE_STATUS.FINISHED:
            return failed_response(f'{lt.job_info} 已经存在且状态为 [{lt.queue_status}]，不能重复创建，请稍后重试')
        if lt is not None and (await check_restart(lt)):
            return failed_response(f'{lt.job_info} 正在重启，不能重复创建，请稍后重试')
        if (task_schema.resource.cpu <= 0 or task_schema.resource.memory <= 0) \
                and task_schema.resource.group.startswith(SHARED_NODE_GROUP):
            return failed_response(f'CPU 或 memory 参数不合法，需大于零')
        if task_schema.resource.is_spot and task_schema.resource.group.startswith(SHARED_NODE_GROUP):
            return failed_response('spot 容器不能创建在共享分组中')
        # 外部独占开发机申请前的检查
        if user.is_external and not task_schema.resource.group.startswith(SHARED_NODE_GROUP):
            success, msg = await check_external_dedicated_jupyter(user, task_schema)
            if not success:
                return failed_response(msg)
        # image 有 "/" 视为自定义镜像, 否则视为内建镜像
        if '/' in str(image := task_schema.resource.image):
            train_image, template = image, f'train_image:{image.split("/")[-1]}'
        else:
            train_image, template = None, image
        if (err := await check_environment_get_err(train_image, template, user)) is not None:
            return failed_response(err)

        if (err := check_services_config_get_err(task_schema.services, user)) is not None:
            return failed_response(err)

        task = BaseTask(
            implement_cls=AioDbOperationImpl, id=None, nb_name=task_schema.name, user_name=user.user_name,
            code_file='stub.sh', workspace='/marsv2/scripts',
            config_json={
                'schema': raw_task_schema,
                # 兼容旧版后端 防止回滚失败, 下个版本删除
                'services': {svc.name: svc.dict() for svc in task_schema.services},
                # deprecated, 兼容旧版前端, 以下字段预期仅有前端使用
                'memory': task_schema.resource.memory,
                'cpu': task_schema.resource.cpu,
                'is_spot': task_schema.resource.is_spot,
                'train_image': task_schema.resource.image if '/' in str(task_schema.resource.image) else None,
                'version': CONF.jupyter.current_version,
            },
            group=task_schema.resource.group, nodes=1, backend=template, queue_status=QUE_STATUS.QUEUED,
            priority=TASK_PRIORITY.AUTO.value, task_type=TASK_TYPE.JUPYTER_TASK, whole_life_state=0,
            mount_code=task_schema.options.get('mount_code', 2)
        )
        try:
            task = await task.create(remote_apply=True)
            await clear_visible_tag(nb_name=task.nb_name, user_name=task.user_name)
            await task.tag_task(tag=VISIBLE_TASK_TAG)
        except Exception:
            if not task:
                return {
                    'success': 0,
                    'msg': '未能在数据库中成功创建队列，请联系系统组'
                }
        return {
            'success': 1,
            'msg': '直接插入队列成功，请等待调度',
            'taskid': task.id,
            'task': task.trait_dict()
        }


async def delete_task_api(task: BaseTask = Depends(get_api_task())):
    task.re_impl(AioDbOperationImpl)
    await task.untag_task(tag=VISIBLE_TASK_TAG)
    return {
        'success': 1,
        'msg': f"delete {task.nb_name} 成功"
    }


async def move_node_api(group: str, user: User = Depends(get_api_user_with_token())):
    await user.quota.create_quota_df()
    if not user.in_group('can_suspend'):
        return {
            'success': 0,
            'result': "failed",
            'msg': '您无权挪动节点打断任务'
        }
    cluster_df = await async_get_nodes_df()
    prefix = (meta_group + '.') if (meta_group := CONF.jupyter.get('node_meta_group')) else ''
    dedicated_group_label = f'{prefix}{user.user_name}_dedicated'
    if len(cluster_df[
               (cluster_df.group == dedicated_group_label) &
               cluster_df.origin_group.str.endswith(group) &
               cluster_df.working.apply(lambda w: w is None).astype(bool)
           ]):
        # 这个用户还有挪出来又没用的节点
        return {
            'success': 0,
            'result': "failed",
            'msg': f'您已经有从 {group} 挪出来的节点了'
        }
    nodes_df = cluster_df[(cluster_df.group == group) & (cluster_df.status == 'Ready')]
    if len(nodes_df) == 0:
        return {
            'success': 0,
            'result': "failed",
            'msg': '该分组没有 Ready 的节点了'
        }
    # 就挪第一个节点
    node = nodes_df.name.to_list()[0]
    res = await async_set_node_label(node, MARS_GROUP_FLAG, dedicated_group_label)
    if res:
        return {
            'success': 1,
            'result': "success",
            'msg': f'成功挪动节点 {node}'
        }
    return {
        'success': 0,
        'result': "failed",
        'msg': f'挪动节点 {node} 失败'
    }


