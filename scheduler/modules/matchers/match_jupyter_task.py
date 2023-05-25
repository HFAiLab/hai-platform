
from itertools import chain
from typing import Dict

import pandas as pd
from conf import CONF
from conf.flags import QUE_STATUS, USER_ROLE, TASK_TYPE
from scheduler.base_model import modify_task_df_safely, MATCH_RESULT, ASSIGN_RESULT


SHARED_NODE_GROUP = CONF.jupyter.shared_node_group_prefix


def match_task(resource_df: pd.DataFrame, task_df: pd.DataFrame, extra_data: Dict):
    resource_df = resource_df[
        (resource_df.status == 'Ready') &
        (resource_df.nodes == 1) &
        (resource_df.working.apply(lambda w: w is None).astype(bool) | (resource_df.working == 'jupyter'))
        ].copy()
    resource_df['n_running_tasks'] = 0
    # note: 先只支持单节点任务
    # 选出能继续运行的任务
    task_df.loc[(task_df.assign_result == ASSIGN_RESULT.CAN_RUN) & (task_df.queue_status == QUE_STATUS.SCHEDULED), 'match_result'] = MATCH_RESULT.KEEP_RUNNING
    keep_running_df = task_df[task_df.match_result == MATCH_RESULT.KEEP_RUNNING]
    # 资源减掉
    for _, task in keep_running_df.iterrows():
        if (memory := task.config_json['schema'].get('resource', {}).get('memory', 0)) == 0:
            resource_df.loc[resource_df.name.isin(task.assigned_nodes), ['nodes', 'memory']] = [0, 0]
        else:
            resource_df.loc[resource_df.name.isin(task.assigned_nodes), 'nodes'] = 0
            resource_df.loc[resource_df.name.isin(task.assigned_nodes), 'memory'] -= memory << 30
            resource_df.loc[resource_df.name.isin(task.assigned_nodes), 'n_running_tasks'] += 1
    running_nodes = set(keep_running_df.assigned_nodes.explode())
    error_nodes = running_nodes - set(resource_df.name)
    exploded_nodes_series = task_df.assigned_nodes.explode()
    task_df.loc[
        (task_df.match_result == MATCH_RESULT.KEEP_RUNNING) &
        task_df.index.isin(exploded_nodes_series[exploded_nodes_series.isin(error_nodes)].index),
        ['match_result', 'scheduler_msg']] = [MATCH_RESULT.SUSPEND, f'本次调度错误节点: {error_nodes}']
    # 选出要启动的任务
    task_df.loc[(task_df.assign_result == ASSIGN_RESULT.CAN_RUN) & (task_df.queue_status == QUE_STATUS.QUEUED), 'match_result'] = MATCH_RESULT.STARTUP
    shared_task_df = task_df[(task_df.group.str.startswith(SHARED_NODE_GROUP)) & (task_df.match_result == MATCH_RESULT.STARTUP)]
    non_shared_task_df = task_df[(~task_df.group.str.startswith(SHARED_NODE_GROUP)) & (task_df.match_result == MATCH_RESULT.STARTUP)]
    # 共享逻辑
    for ind, task in shared_task_df.sort_index().iterrows():
        cpu, memory = (task.config_json['schema'].get('resource', {}).get(key, 0) for key in ['cpu', 'memory'])
        # 暂时只支持单节点任务，用 max 空数组会报错
        nodes_df = resource_df[(resource_df.group == task.group) & (resource_df.memory >= (memory << 30))].sort_values('n_running_tasks', ascending=True)
        if len(nodes_df):
            nodes_df = nodes_df[0:1]
            if task.group.startswith(CONF.jupyter.mig_node_group_prefix):
                assigned_gpus_list = [[task.assigned_gpus]]
            else:
                assigned_gpus_list = [[i for i in range(gpus)] for gpus in nodes_df.gpu_num.to_list()]
            task_df = modify_task_df_safely(
                task_df,
                task_id=ind,
                assigned_nodes=nodes_df.name.to_list(),
                memory=[memory << 30],
                cpu=[cpu],
                assigned_gpus=assigned_gpus_list
            )
            resource_df.loc[resource_df.index.isin(nodes_df.index), 'memory'] -= memory << 30
            resource_df.loc[resource_df.index.isin(nodes_df.index), 'n_running_tasks'] += 1
        else:
            task_df.loc[task_df.index == ind, 'match_result'] = MATCH_RESULT.DO_NOTHING
    # 独占逻辑
    for ind, task in non_shared_task_df.sort_index().iterrows():
        # 先尝试用自己分组的节点
        nodes_df = resource_df[
            (
                (resource_df.group == f'{task.user_name}_dedicated') &
                (resource_df.origin_group.str.endswith(task.group))
            ) &
            (resource_df.nodes > 0)
            ]
        if len(nodes_df) == 0:
            # 没有自己分组的节点, 找其他节点, 但尽量避开 background task 在运行的节点
            bg_task_node_set = extra_data.get('bg_task_node_set', set())
            nodes_df = resource_df[(resource_df.group == task.group) & (resource_df.nodes > 0)]
            nodes_df = nodes_df.sort_values(by='name', key=lambda s: s.apply(lambda x: int(x in bg_task_node_set)))
        if len(nodes_df):
            nodes_df = nodes_df[0:1]
            task_df = modify_task_df_safely(
                task_df,
                task_id=ind,
                assigned_nodes=nodes_df.name.to_list(),
                memory=nodes_df.memory.to_list(),
                cpu=[0],
                assigned_gpus=[[i for i in range(gpus)] for gpus in nodes_df.gpu_num.to_list()]
            )
            resource_df.loc[resource_df.index.isin(nodes_df.index), 'nodes'] = 0
        else:
            task_df.loc[task_df.index == ind, 'match_result'] = MATCH_RESULT.DO_NOTHING
    # 没权利跑，又还在跑的，打断
    task_df.loc[(task_df.assign_result != ASSIGN_RESULT.CAN_RUN) & (task_df.queue_status == QUE_STATUS.SCHEDULED),
                ['assign_result', 'match_result']] = [ASSIGN_RESULT.CAN_NOT_RUN, MATCH_RESULT.SUSPEND]
    # 没权利跑，但还在排队的外部用户独占容器，直接停止
    task_df.loc[(task_df.assign_result != ASSIGN_RESULT.CAN_RUN) & (task_df.queue_status == QUE_STATUS.QUEUED) & \
                (task_df.user_role == USER_ROLE.EXTERNAL) & (~task_df.group.str.startswith(SHARED_NODE_GROUP)),
                ['match_result']] = MATCH_RESULT.STOP
    # 没做处理的任务都标记为不可运行
    task_df.loc[~task_df.match_result.isin({MATCH_RESULT.KEEP_RUNNING, MATCH_RESULT.STARTUP, MATCH_RESULT.SUSPEND, MATCH_RESULT.STOP}),
                ['assign_result', 'match_result']] = [ASSIGN_RESULT.CAN_NOT_RUN, MATCH_RESULT.DO_NOTHING]
    return resource_df, task_df
