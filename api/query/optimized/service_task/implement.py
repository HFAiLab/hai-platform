
from .default import *
from .custom import *

import asyncio

import pandas as pd
from fastapi import Depends

from conf import CONF
from api.task.service_task import QUOTA_SPOT_JUPYTER, QUOTA_DEDICATED_JUPYTER, get_spot_jupyter_status, VISIBLE_TASK_TAG
from api.depends import JUPYTER_ADMIN_GROUP, get_api_user_with_token
from conf.flags import QUE_STATUS, TASK_TYPE, EXP_STATUS
from db import MarsDB
from k8s.async_v1_api import async_get_nodes_df
from monitor.utils import async_redis_cached
from server_model.user import User
from server_model.user_impl.user_access import ACCESS_SCOPE
from server_model.user_data import UserAccessTokenTable


def construct_sql(filters=None, fullmatch_filters=None, universal_filter_keywords=None, select_count=False):
    filters, fullmatch_filters = filters or dict(), fullmatch_filters or dict()
    make_filter_sql = lambda columns: 'and'.join([
        f''' "{column}" ~ '{filters[column]}' '''
        for column in columns if filters.get(column) is not None
    ] + [
        f''' "{column}" = '{fullmatch_filters[column]}' '''
        for column in columns if fullmatch_filters.get(column) is not None
    ])
    inner_filter_sql = make_filter_sql(columns=['group', 'nb_name'])
    outer_filter_sql = make_filter_sql(columns=['node', 'user_name', 'status'])

    if universal_filter_keywords is not None and len(universal_filter_keywords) > 0:
        universal_filter_sql = 'and'.join(
            f''' "res"."all_filter_fields" ~ '{keyword}' '''
            for keyword in universal_filter_keywords
        )
        if len(outer_filter_sql) == 0:
            outer_filter_sql = universal_filter_sql
        else:
            outer_filter_sql += ' and ' + universal_filter_sql

    return f"""
        select {'count(*)' if select_count else '*'} from (
            select *, concat_ws(' ', "user_name", "nb_name", "node", "group", "status") as "all_filter_fields"
            from (
                select
                    "task_ng".*,
                    "assigned_nodes"[1] as "node",
                    "t_rc"."runtime_config_json",
                    case
                        when "pod"."status" is null then
                            case
                                when "task_ng"."queue_status" = 'finished' then 'stopped'
                                else "task_ng"."queue_status"
                            end
                        when "pod"."status" like '%_terminating' then 'terminating'
                        when "pod"."status" = any('{{ {",".join(EXP_STATUS.FINISHED)} }}'::varchar[]) then 'stopped'
                        else "pod"."status"
                    end as "status"
                from "task_ng"
                inner join "task_tag" "tt" on "task_ng"."chain_id" = "tt"."chain_id"
                left join "pod_ng" "pod" on "pod"."task_id" = "task_ng"."id" and "pod"."job_id" = 0
                left join (
                    select "id", coalesce(jsonb_object_agg("source", "config_json") filter ( where "source" is not null ), '{{}}'::jsonb) as "runtime_config_json"
                    from (
                        select "task_ng"."id", "tr"."source", "tr"."config_json"
                        from "task_ng"
                        inner join "task_tag" "tt" on "task_ng"."chain_id" = "tt"."chain_id"
                        left join "task_runtime_config" "tr" on "tr"."task_id" = "task_ng"."id" or "tr"."chain_id" = "task_ng"."chain_id"
                        where "task_type" = '{TASK_TYPE.JUPYTER_TASK}' and last_task = true and "tt"."tag" = '{VISIBLE_TASK_TAG}'
                    ) as "t" group by "id"
                ) "t_rc" on "t_rc"."id" = "task_ng"."id"
                where "task_type" = 'jupyter' and "last_task" = true and "tt"."tag" = '{VISIBLE_TASK_TAG}' 
                    {('and ' + inner_filter_sql) if inner_filter_sql else ''}
            ) as "res_before_concat"
        ) as "res" {outer_filter_sql and f'where {outer_filter_sql}'}
    """


async def async_get_jupyter_task_df(limit=1000, offset=0, filters=None, fullmatch_filters=None, universal_filter_keywords=None):
    sql = f"""
        {construct_sql(filters, fullmatch_filters, universal_filter_keywords, select_count=False)}
    """
    res = await MarsDB().a_execute(sql)
    df = pd.DataFrame([{**r} for r in res])
    if len(df) == 0:
        df = pd.DataFrame([], columns=['id', 'nb_name', 'user_name', 'code_file', 'workspace', 'config_json',
                                       'group', 'nodes', 'assigned_nodes', 'node', 'restart_count', 'whole_life_state',
                                       'first_id', 'backend', 'task_type', 'queue_status', 'notes', 'priority',
                                       'chain_id', 'stop_code', 'suspend_code', 'mount_code',
                                       'suspend_updated_at', 'begin_at', 'end_at', 'created_at',
                                       'worker_status', 'status'])
    df = df.sort_values('id', ascending=False)
    df = df[offset:offset+limit]
    last_checkpoint_list = []
    # 一次性拿所有的 image
    sql = """
    select distinct on ("user_name", "description") * 
    from "user_image" 
    order by "user_name", "description", "updated_at" desc
    """
    res = await MarsDB().a_execute(sql)
    image_dict = {f'{r.user_name}-{r.description}': {**r} for r in res}
    for _, row in df.iterrows():
        # 拿 image 只需要用户名
        last_checkpoint_image = image_dict.get(f'{row.user_name}-{row.nb_name}')
        last_checkpoint_list.append(last_checkpoint_image['updated_at'].strftime("%Y-%m-%d %H:%M:%S") if last_checkpoint_image else None)
    df['last_checkpoint'] = last_checkpoint_list
    return df


def get_jupyter_tasks_info(jupyter_task_df: pd.DataFrame, node_ip):
    jupyter_tasks = dict()
    for _, row in jupyter_task_df.iterrows():
        task_name = f'{row.user_name}/{row.nb_name}'
        task_info = dict(row.to_dict())
        # Task 的全字段
        jupyter_tasks[task_name] = task_info
        # 添加额外字段
        task_node_ip =  node_ip.get(row.node)
        services = task_info['runtime_config_json'].get('service_task', {}).get('services', {}) or task_info['config_json'].get('services', {})   # deprecating
        jupyter_tasks[task_name].update({
            # ServiceTask 特有的字段
            'node_ip': task_node_ip if row.queue_status == QUE_STATUS.SCHEDULED else None,
            # Deprecating (为了兼容旧接口的额外字段)
            'builtin_services':
                [{'name': svc_name, **svc} for svc_name, svc in services.items() if svc_name in CONF.jupyter.builtin_services],
            'custom_services':
                [{'name': svc_name, **svc} for svc_name, svc in services.items() if svc_name not in CONF.jupyter.builtin_services],
        })
    return jupyter_tasks


async def async_get_num_jupyter_tasks(filters=None, fullmatch_filters=None, universal_filter_keywords=None):
    sql = construct_sql(filters, fullmatch_filters, universal_filter_keywords, select_count=True)
    res = await MarsDB().a_execute(sql)
    return next(res).count


@async_redis_cached(ttl_in_sec=5)
async def async_get_cluster_status():
    # 计算各个 group 的剩余资源
    all_jupyter_task_df = await async_get_jupyter_task_df()
    cluster_df = await async_get_nodes_df()
    cluster_df = cluster_df[(cluster_df.working.apply(lambda w: w is None).astype(bool) | (cluster_df.working == TASK_TYPE.JUPYTER_TASK)) & (cluster_df.status == 'Ready')]
    for _, row in all_jupyter_task_df[all_jupyter_task_df.queue_status == QUE_STATUS.SCHEDULED].iterrows():
        cluster_df.loc[cluster_df.name == row.node, 'memory'] -= row.config_json['memory'] << 30
    not_working_dict = cluster_df[cluster_df.working.apply(lambda w: w is None).astype(bool)].groupby('group').name.count().to_dict()
    allocatable_memory_dict = cluster_df.groupby('group').memory.max().to_dict()
    cpu_dict = cluster_df.groupby('group').cpu.max().to_dict()
    resource = {}
    for group in cpu_dict:
        resource[group] = {
            'not_working': not_working_dict.get(group, 0),
            'allocatable': allocatable_memory_dict.get(group, 0) >> 30,
            'max_cpu_core': cpu_dict.get(group, 0),
        }
    node_ip = cluster_df[['name', 'internal_ip']].set_index('name').to_dict().get('internal_ip', {})
    return {'resource': resource, 'node_ip': node_ip}


async def data_api(user: User = Depends(get_api_user_with_token())):
    # 处理 Jupyter quota
    await user.quota.create_quota_df()
    jupyter_quota = user.quota.jupyter_quota
    tmp = {}
    for g in sorted(jupyter_quota.keys(), key=lambda k: ~k.startswith(CONF.jupyter.shared_node_group_prefix)):
        jupyter_quota[g]['allocatable'] = 0
        jupyter_quota[g]['running'] = 0
        jupyter_quota[g]['not_working'] = 0
        tmp[g] = jupyter_quota[g]
    jupyter_quota = tmp

    user_jupyter_task_df = await async_get_jupyter_task_df(fullmatch_filters={'user_name': user.user_name})
    for _, row in user_jupyter_task_df.iterrows():
        if not jupyter_quota.get(row.group):
            jupyter_quota[row.group] = {
                'cpu': 0,
                'memory': 0,
                'quota': 0,
                'allocatable': 0,
                'running': 0,
                'not_working': 0
            }
        if row.status != 'stopped':
            jupyter_quota[row.group]['running'] += 1

    cluster_status = await async_get_cluster_status()
    for g in jupyter_quota:
        if g in cluster_status['resource']:
            jupyter_quota[g].update(cluster_status['resource'][g])
    jupyter_tasks = get_jupyter_tasks_info(user_jupyter_task_df, cluster_status['node_ip'])

    # 处理 node port 信息
    nodeports = await user.nodeport.async_get()
    for nb_name, node_port_list in nodeports.items():
        node_ip = jupyter_tasks.get(f'{user.user_name}/{nb_name}', {}).get('node_ip', None)
        for port in node_port_list:
            port['ip'] = node_ip

    result = {
        'tasks': list(jupyter_tasks.values()),
        'quota': jupyter_quota,
        'nodeports': nodeports,
        'nodeport_quota': user.nodeport.quota_info(),
        'environments': user.quota.train_environments,
        'admin': user.in_group(JUPYTER_ADMIN_GROUP),
        'can_suspend': user.in_group('can_suspend'),
    }
    result.update(await extra_data(user))
    if not user.is_internal:
        result.update({
            'spot_jupyter_quota': int(user.quota.quota(QUOTA_SPOT_JUPYTER)),
            'spot_jupyter_status': await get_spot_jupyter_status(),
            'dedicated_jupyter_quota': int(user.quota.quota(QUOTA_DEDICATED_JUPYTER)),
        })

    return {
        'success': 1,
        'msg': 'success',
        'result': result
    }


async def all_tasks_api(user: User = Depends(get_api_user_with_token(allowed_groups=[JUPYTER_ADMIN_GROUP])),
                        page: int = 1, page_size: int = 1000,
                        user_name: str = None, nb_name: str = None, node: str = None,
                        group: str = None, status: str = None, universal_filter_keywords: str = None):
    cluster_df_task = asyncio.create_task(async_get_nodes_df())
    filters = {'user_name':user_name, 'nb_name':nb_name, 'node':node, 'group':group, 'status':status}
    uni_keywords = universal_filter_keywords.split() if universal_filter_keywords is not None else []
    jupyter_df_task = asyncio.create_task(async_get_jupyter_task_df(
        limit=page_size, offset=(page-1)*page_size, filters=filters,
        universal_filter_keywords=uni_keywords
    ))
    jupyter_count_task = asyncio.create_task(
        async_get_num_jupyter_tasks(filters=filters, universal_filter_keywords=uni_keywords)
    )
    # 为 task 增加 token 信息方便直接访问 Jupyter
    user_token_df = await UserAccessTokenTable.async_df
    user_token_df = user_token_df[
        (user_token_df.access_user_name == user_token_df.from_user_name) &
        (user_token_df.access_scope == ACCESS_SCOPE.ALL) &
        user_token_df.active
    ]
    user_token = {user_name: token for user_name, token in zip(user_token_df['access_user_name'], user_token_df['access_token'])}
    jupyter_task_df = await jupyter_df_task
    jupyter_task_df['token'] = jupyter_task_df.user_name.apply(lambda u: user_token.get(u, None))
    # admin 接口不在前端轮询查看是否可以访问
    jupyter_task_df.status = jupyter_task_df.status.apply(lambda s: s if s != 'standby' else 'ready')
    return {
        'success': 1,
        'msg': 'success',
        'result': {
            'tasks': list(get_jupyter_tasks_info(jupyter_task_df, await cluster_df_task).values()),
            'total_count': await jupyter_count_task,
        }
    }

