

from .default import *
from .custom import *

import datetime
from typing import List
from fastapi import Depends, Query

from api.depends import get_api_user_with_token
from base_model.training_task import TrainingTask
from conf import CONF
from conf.flags import QUE_STATUS, CHAIN_STATUS, TASK_TYPE
from db import MarsDB
from server_model.auto_task_impl import AutoTaskApiImpl
from server_model.user import User
from utils import convert_to_external_task
try:
    from monitor import async_get_container_monitor_stats
except:
    pass


async def get_chain_tasks_in_query_db(
        sql_where_and,
        sql_where_and_args,
        page: int = None,
        page_size: int = None,
        user: User = None,
        select_pods=False,
        order_by='id',
        count=True
):
    sql = f"""
    select 
        "t".*, "task_ng".*, coalesce("tt"."tags", '{{}}')::varchar[] as "tags",
        case 
            when "task_ng"."queue_status" = '{QUE_STATUS.FINISHED}' then '{CHAIN_STATUS.FINISHED}'
            when "task_ng"."queue_status" = '{QUE_STATUS.QUEUED}' and "task_ng"."id" != "task_ng"."first_id" then '{CHAIN_STATUS.SUSPENDED}'
            when "task_ng"."queue_status" = '{QUE_STATUS.QUEUED}' and "task_ng"."id" = "task_ng"."first_id" then '{CHAIN_STATUS.WAITING_INIT}'
            when "task_ng"."queue_status" = '{QUE_STATUS.SCHEDULED}' then '{CHAIN_STATUS.RUNNING}'
        end as "chain_status",
        "task_ng"."config_json" as "config_json",
        coalesce(jsonb_object_agg("tr"."source", "tr"."config_json") filter ( where "tr"."source" is not null ), '{{}}'::jsonb) as "runtime_config_json"
    from "task_ng"
    inner join (
        select
            max("id") as "max_id",
            array_agg("id" order by "id") as "id_list",
            array_agg("queue_status" order by "id") as "queue_status_list",
            array_agg("begin_at" order by "id") as "begin_at_list",
            array_agg("end_at" order by "id") as "end_at_list",
            array_agg("created_at" order by "id") as "created_at_list",
            array_agg("stop_code" order by "id") as "stop_code_list",
            array_agg("suspend_code" order by "id") as "suspend_code_list",
            array_agg("whole_life_state" order by "id") as "whole_life_state_list",
            array_agg("worker_status" order by "id") as "worker_status_list"
        from "task_ng"
        where "task_ng"."chain_id" in (
            select
                "chain_id"
            from "task_ng"
            where "last_task" {sql_where_and}
            order by "{order_by}" desc
            {'' if page is None else f'limit {page_size} offset {page_size * (page - 1)}'}
        )
        group by "task_ng"."chain_id"
    ) as "t" on "t"."max_id" = "task_ng"."id"
    left join (
        select array_agg("task_tag"."tag") as "tags", "chain_id"
        from "task_tag"
        group by "chain_id"
    ) "tt" on "tt"."chain_id" = "task_ng"."chain_id"
    left join "task_runtime_config" "tr" on "tr"."task_id" = "task_ng"."id" or "tr"."chain_id" = "task_ng"."chain_id"
    group by
        "task_ng"."id", "t"."max_id", "t"."id_list", "t"."queue_status_list", "t"."begin_at_list", "t"."end_at_list",
        "t"."created_at_list", "t"."stop_code_list", "t"."suspend_code_list", "t"."whole_life_state_list",
        "t"."worker_status_list", "tt"."tags"
    order by "{order_by}" desc
    """
    results = (await MarsDB().a_execute(sql, sql_where_and_args)).fetchall()
    if count:
        count_sql = f"""
            select
                count(*)
            from "task_ng"
            where "last_task" {sql_where_and}
            """
        total_count = (await MarsDB().a_execute(count_sql, sql_where_and_args)).fetchall()[0]['count']
    else:
        total_count = len(results)
    res = []
    for r in results:
        task = TrainingTask(AutoTaskApiImpl, **{**r})
        if select_pods:
            await task.aio_select_pods()
        if user is not None and not user.is_internal:
            task = convert_to_external_task(task)
        res.append(task.trait_dict())
    return res, total_count


async def get_tasks_api(
        page: int,
        page_size: int,
        task_type: List[str] = Query(default=None),
        nb_name_pattern: str = None,
        worker_status: List[str] = Query(default=None),
        queue_status: List[str] = Query(default=None),
        tag: List[str] = Query(default=None),
        excluded_tag: List[str] = Query(default=None),
        group: List[str] = Query(default=None),
        only_star: bool = False,
        select_pods: bool = True,
        created_start_time: str = None,
        created_end_time: str = None,
        order_by: str = 'id',
        user: User = Depends(get_api_user_with_token())
):
    if order_by not in {'id', 'first_id'}:
        return {
            'success': 0,
            'msg': f'order_by 只能是 id / first_id'
        }
    if page <= 0:
        return {
            'success': 0,
            'msg': 'page 必须大于等于 1'
        }
    if page_size <= 0 or page_size > 100:
        return {
            'success': 0,
            'msg': 'page_size 需要为 1 ~ 100'
        }
    if any((created_start_time, created_end_time)) and not all((created_start_time, created_end_time)):
        return {
            'success': 0,
            'msg': 'created_start_time 和 created_end_time 必须同时为空 / 不为空'
        }
    if created_start_time is not None and created_end_time is not None:
        try:
            created_start_time = datetime.datetime.fromisoformat(created_start_time)
            created_end_time = datetime.datetime.fromisoformat(created_end_time)
        except Exception:
            return {
                'success': 0,
                'msg': 'created_start_time / created_end_time 格式不正确，需要为 isoformat'
            }
    sql_where_and = ' and "user_name" = %s '
    sql_where_and_args = (user.user_name, )
    if task_type is not None:
        sql_where_and += f''' and "task_type" in ({",".join("%s" for _ in task_type)}) '''
        sql_where_and_args += tuple(task_type)
    if group is not None:
        client_groups = [g for g in group if '#' in g]
        groups = [g.split('#')[0] if '#' in g else g for g in group]
        sql_where_and += f''' and "group" in ({",".join("%s" for _ in groups)}) '''
        sql_where_and_args += tuple(groups)
        # 有 # 的 groups
        if len(client_groups) > 0:
            sql_where_and += f''' and "config_json"->>'client_group' in ({",".join("%s" for _ in client_groups)}) '''
            sql_where_and_args += tuple(client_groups)
    if nb_name_pattern is not None:
        sql_where_and += ' and ("nb_name" like %s or "id"::varchar like %s) '
        sql_where_and_args += (f'%{nb_name_pattern}%', f'{nb_name_pattern}%', )
    if worker_status is not None:
        sql_where_and += f''' and "worker_status" in ({",".join("%s" for _ in worker_status)}) '''
        sql_where_and_args += tuple(worker_status)
    if queue_status is not None:
        sql_where_and += f''' and "queue_status" in ({",".join("%s" for _ in queue_status)}) '''
        sql_where_and_args += tuple(queue_status)
    if only_star and tag is None:
        tag = ['star']
    if tag is not None:
        sql_where_and += f' and "chain_id" in (select "chain_id" from "task_tag" where "user_name" = %s and "tag" in ({",".join("%s" for _ in tag)})) '
        sql_where_and_args += (user.user_name, ) + tuple(tag)
    if excluded_tag is not None:
        sql_where_and += f' and "chain_id" not in (select "chain_id" from "task_tag" where "user_name" = %s and "tag" in ({",".join("%s" for _ in excluded_tag)})) '
        sql_where_and_args += (user.user_name, ) + tuple(excluded_tag)
    if created_start_time is not None and created_end_time is not None:
        sql_where_and += f"""
        and "chain_id" in (select "chain_id" from "task_ng" where "user_name" = %s and "created_at" > %s and "created_at" < %s and "restart_count" = 0)
        """
        sql_where_and_args += (user.user_name, created_start_time, created_end_time)
    results, total_count = await get_chain_tasks_in_query_db(
        sql_where_and=sql_where_and,
        sql_where_and_args=sql_where_and_args,
        page=page,
        page_size=page_size,
        user=user,
        select_pods=select_pods,
        order_by=order_by
    )
    return {
        'success': 1,
        'result': {
            'tasks': results,
            'total': total_count
        }
    }


async def get_task_api(
        id: int = None,
        chain_id: str = None,
        nb_name: str = None,
        user: User = Depends(get_api_user_with_token())
):
    sql_where_and = ' and "user_name" = %s '
    sql_where_and_args = (user.user_name, )
    if id is not None:
        sql_where_and += ' and "chain_id" in (select "chain_id" from "task_ng" where "id" = %s) '
        sql_where_and_args += (id, )
    elif chain_id is not None:
        sql_where_and += ' and "chain_id" = %s '
        sql_where_and_args += (chain_id, )
    elif nb_name is not None:
        sql_where_and += ' and "nb_name" = %s '
        sql_where_and_args += (nb_name, )
    else:
        return {
            'success': 0,
            'msg': '必须指定 id, chain_id 或者 nb_name'
        }
    results, _ = await get_chain_tasks_in_query_db(sql_where_and=sql_where_and, sql_where_and_args=sql_where_and_args, page=1, page_size=1, user=user, select_pods=True, count=False)
    if len(results) == 0:
        return {
            'success': 0,
            'msg': '没有符合条件的任务'
        }
    return {
            'success': 1,
            'result': {
                'task': results[0]
            }
        }


async def get_time_range_schedule_info_api(start_time: str, end_time: str, user: User = Depends(get_api_user_with_token())):
    extra_join = '' if user.is_internal else """
    inner join "user" on "user"."user_name" = "task_ng"."user_name" and "user"."role" = 'external'
    """
    sql = f"""
    select
       count(*) as "count", {'"group"' if user.is_internal else ''' 'training' as "group"'''}, 'created' as "tag"
    from "task_ng"
    {extra_join}
    where
        "created_at" >= '{start_time}' and "created_at" < '{end_time}' and
        "id" = "first_id" and "task_type" = '{TASK_TYPE.TRAINING_TASK}' {'' if user.is_internal else f''' and "group" = '{CONF.scheduler.default_group}' '''}
    group by "group"
    union all
    select
           count(*) as "count", {'"group"' if user.is_internal else ''' 'training' as "group"'''}, 'finished' as "tag"
    from "task_ng"
    {extra_join}
    where
        "created_at" >= '{start_time}' and "created_at" < '{end_time}' and
        "queue_status" = '{QUE_STATUS.FINISHED}' and task_type = '{TASK_TYPE.TRAINING_TASK}' and
        "chain_id" not in (select "chain_id" from "task_ng" where "queue_status" = '{QUE_STATUS.QUEUED}' or "queue_status" = '{QUE_STATUS.SCHEDULED}') {'' if user.is_internal else f''' and "group" = '{CONF.scheduler.default_group}' '''}
    group by "group"
    """
    results = await MarsDB().a_execute(sql)
    res = {
        'created': 0,
        'finished': 0,
        'detail': {
            'created': [],
            'finished': []
        }
    }
    for r in results.fetchall():
        count, group, tag = r['count'], r['group'], r['tag']
        if tag == 'created':
            res['created'] += count
            res['detail']['created'].append({
                'count': count,
                'group': group
            })
        elif tag == 'finished':
            res['finished'] += count
            res['detail']['finished'].append({
                'count': count,
                'group': group
            })
    return {
        'success': 1,
        'result': res
    }


async def get_running_tasks_api(
        task_type: List[str] = Query(default=None),
        user: User = Depends(get_api_user_with_token())
):
    if not user.is_internal:
        return {
            'success': 0,
            'msg': '无权访问'
        }
    if task_type is None:
        task_type = TASK_TYPE.all_task_types()
    results = await MarsDB().a_execute(f"""
    select 
        "user"."role" as "user_role", "unfinished_task_ng"."id", "unfinished_task_ng"."nb_name", "unfinished_task_ng"."queue_status",
        "unfinished_task_ng"."user_name", "unfinished_task_ng"."priority", "unfinished_task_ng"."assigned_nodes",
        "unfinished_task_ng"."nodes", "unfinished_task_ng"."backend", "unfinished_task_ng"."begin_at", "unfinished_task_ng"."created_at",
        "unfinished_task_ng"."group", "unfinished_task_ng"."chain_id", "unfinished_task_ng"."task_type", "unfinished_task_ng"."first_id",
        case 
            when "unfinished_task_ng"."queue_status" = '{QUE_STATUS.FINISHED}' then '{CHAIN_STATUS.FINISHED}'
            when "unfinished_task_ng"."queue_status" = '{QUE_STATUS.QUEUED}' and "unfinished_task_ng"."id" != "unfinished_task_ng"."first_id" then '{CHAIN_STATUS.SUSPENDED}'
            when "unfinished_task_ng"."queue_status" = '{QUE_STATUS.QUEUED}' and "unfinished_task_ng"."id" = "unfinished_task_ng"."first_id" then '{CHAIN_STATUS.WAITING_INIT}'
            when "unfinished_task_ng"."queue_status" = '{QUE_STATUS.SCHEDULED}' then '{CHAIN_STATUS.RUNNING}'
        end as "chain_status",
        "unfinished_task_ng"."config_json" as "config_json",
        coalesce(jsonb_object_agg("tr"."source", "tr"."config_json") filter ( where "tr"."source" is not null ), '{{}}'::jsonb) as "runtime_config_json"
    from "unfinished_task_ng"
    inner join (
        select
            max("id") as "max_id"
        from "unfinished_task_ng"
        where "chain_id" in (
            select distinct on ("chain_id", "first_id") "chain_id"
            from "unfinished_task_ng"
            where "task_type" in ({",".join("%s" for _ in task_type)}) and ("queue_status" = '{QUE_STATUS.QUEUED}' or "queue_status" = '{QUE_STATUS.SCHEDULED}')
        )
        group by "chain_id"
    ) as "t" on "t"."max_id" = "unfinished_task_ng"."id"
    inner join "user" on "user"."user_name" = "unfinished_task_ng"."user_name"
    left join "task_runtime_config" "tr" on "tr"."task_id" = "unfinished_task_ng"."id" or "tr"."chain_id" = "unfinished_task_ng"."chain_id"
    group by "unfinished_task_ng"."id","unfinished_task_ng","first_id", "user"."role"
    order by "first_id" desc
    """, tuple(task_type))
    return {
        'success': 1,
        'result': results.fetchall()
    }


async def get_task_container_monitor_stats_api(user: User = Depends(get_api_user_with_token())):
    res = await async_get_container_monitor_stats()
    return {
        'success': 1,
        'result': res
    }


async def get_tasks_overview(user: User = Depends(get_api_user_with_token())):
    result = await MarsDB().a_execute("""
    select
        coalesce((config_json->'running_priority'->-1->'priority')::integer, "priority") as "priority", "queue_status", count(*)
    from "unfinished_task_ng"
    group by coalesce((config_json->'running_priority'->-1->'priority')::integer, "priority"), "queue_status"
    order by coalesce((config_json->'running_priority'->-1->'priority')::integer, "priority") desc, "queue_status" 
    """)
    return {
        'success': 1,
        'result': [{**r} for r in result]
    }
