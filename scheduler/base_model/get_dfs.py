

import pandas as pd

from conf.flags import EXP_STATUS, QUE_STATUS, TASK_TYPE
from db import MarsDB
from k8s.async_v1_api import async_get_nodes_df
from server_model.user_data import SchedulerUserTable
from scheduler.base_model import ASSIGN_RESULT, MATCH_RESULT, TickData

USER_MAX_TASK_COUNT = 600


SAMPLE_TICK_DATA = TickData()
RESOURCE_DF_COLUMNS = list(set(SAMPLE_TICK_DATA.resource_df.columns) - {'active', 'allocated'})


def get_resource_df(loop):
    df = loop.run_until_complete(async_get_nodes_df(monitor=False))
    df = df[RESOURCE_DF_COLUMNS].copy()
    df['active'] = True
    df['allocated'] = False  # 标记这个节点已经被 validation scheduler 占用了
    return df


def get_task_df():
    sql = f"""
    select
        "tmp".*,
        coalesce(("config_json"->'schema'->'resource'->'is_spot')::bool, false) as "is_spot_jupyter",
        config_json->'assigned_resource'->>'assigned_numa' as "assigned_numa",
        coalesce((runtime_config_json->'runtime_priority'->'custom_rank')::float, "first_id")::float as "custom_rank"
    from (
        select
            "tmp".*,
            coalesce(jsonb_object_agg("tr"."source", "tr"."config_json") filter ( where "tr"."source" is not null ), '{{}}'::jsonb) as "runtime_config_json"
        from (
            select
                "id", "nb_name", "user_name", "code_file", "group",
                "nodes" - coalesce(array_length(array (select unnest("assigned_nodes") intersect select unnest("succeeded_assigned_nodes")), 1), 0) as "nodes",
                case
                    when array_length("succeeded_assigned_nodes", 1) is null then "assigned_nodes"
                    else array (select unnest("assigned_nodes") except select unnest("succeeded_assigned_nodes"))
                end as "assigned_nodes",
                "backend", "task_type", "queue_status", "priority", "first_id",
                extract(epoch from (current_timestamp - "begin_at")) as "running_seconds", "chain_id", "tmp"."config_json", "role" as "user_role",
                '{ASSIGN_RESULT.NOT_SURE}' as "assign_result", '{MATCH_RESULT.NOT_SURE}' as "match_result", '' as "scheduler_msg",
                extract(epoch from (current_timestamp - "created_at")) as "created_seconds", "worker_status", 
                case
                    when "task_type" = '{TASK_TYPE.TRAINING_TASK}' and (select "value" from "multi_server_config" where "module" = 'scheduler' and "key" = 'schedule_to_A') ? "user_name" then 'A'
                    when "task_type" = '{TASK_TYPE.TRAINING_TASK}' and (select "value" from "multi_server_config" where "module" = 'scheduler' and "key" = 'schedule_to_B') ? "user_name" then 'B'
                    else "schedule_zone"
                end as "schedule_zone",
                case when "queue_status" = '{QUE_STATUS.SCHEDULED}' then "current_schedule_zone" end as "current_schedule_zone", "client_group"
            from (
                select
                    "id", "nb_name", "unfinished_task_ng"."user_name", "code_file", "group", "nodes", "assigned_nodes", "backend", "task_type",
                    "queue_status", "priority", "unfinished_task_ng"."begin_at", "notes", "chain_id", "config_json", "user"."role", "first_id",
                    "unfinished_task_ng"."created_at", "worker_status", "config_json"->>'schedule_zone' as "schedule_zone",
                    "config_json"->'client_group' as "client_group", "host"."schedule_zone" as "current_schedule_zone",
                    coalesce(array_agg("pod_ng"."node") filter (where "pod_ng"."status" = '{EXP_STATUS.SUCCEEDED}'), array[]::varchar[]) as "succeeded_assigned_nodes",
                    rank() over (partition by "unfinished_task_ng"."user_name", "group" order by first_id asc) as "rank"
                from "unfinished_task_ng"
                inner join "user" on "unfinished_task_ng"."user_name" = "user"."user_name"
                left join "host" on "unfinished_task_ng"."assigned_nodes"[1] = "host"."node"
                left join "pod_ng" on "unfinished_task_ng"."id" = "pod_ng"."task_id" and "pod_ng"."status" = '{EXP_STATUS.SUCCEEDED}'
                group by "unfinished_task_ng"."id", "host"."node", "user"."role"
            ) as "tmp"
        ) as "tmp"
        left join "task_runtime_config" "tr" on "tr"."task_id" = "tmp"."id" or "tr"."chain_id" = "tmp"."chain_id"
        where "nodes" > 0
        group by
            "tmp"."id", "tmp"."chain_id", "tmp"."nb_name", "tmp"."user_name", "tmp"."code_file", "tmp"."group", "tmp"."nodes",
            "tmp"."assigned_nodes", "tmp"."backend", "tmp"."task_type", "tmp"."queue_status", "tmp"."priority", "tmp"."first_id", "tmp"."config_json",
            "tmp"."worker_status", "tmp"."schedule_zone", "tmp"."current_schedule_zone", "tmp"."client_group",
            "tmp"."running_seconds", "tmp"."user_role", "tmp"."assign_result", "tmp"."match_result",
            "tmp"."scheduler_msg", "tmp"."created_seconds"
    ) as "tmp"
    """
    task_df = pd.DataFrame.from_records([{**res} for res in MarsDB().execute(sql)])
    if len(task_df) == 0:
        task_df = SAMPLE_TICK_DATA.task_df.copy()
    # index 存为 task_id，方便使用
    task_df.index = task_df.id
    task_df.sort_index(inplace=True)
    # 添加 memory cpu 字段
    task_df['memory'] = None
    task_df['cpu'] = None
    task_df['assigned_gpus'] = None
    return task_df


def get_user_df():
    user_df = SchedulerUserTable.df
    if len(user_df) == 0:
        user_df = SAMPLE_TICK_DATA.user_df.copy()
    return user_df
