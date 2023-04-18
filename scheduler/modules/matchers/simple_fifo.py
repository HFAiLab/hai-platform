

import random
import pandas as pd
from conf.flags import QUE_STATUS, TASK_TYPE
from scheduler.base_model import Matcher, ASSIGN_RESULT, MATCH_RESULT


class FIFOMatcher(Matcher):
    """
    一个简单的 FIFO 撮合器示例，从前往后选择尽可能多的可以运行的任务
    """
    re_signal_where = f''' "unfinished_task_ng"."task_type" = '{TASK_TYPE.TRAINING_TASK}' '''
    def __init__(self, reserved_cpu=0, reserved_memory=0, **kwargs):
        self.reserved_cpu = reserved_cpu
        self.reserved_memory = reserved_memory
        super().__init__(**kwargs)

    def process_match(self):
        self.set_tick_data(self.waiting_for_upstream_data())
        available_resource_df = self.resource_df[
            (self.resource_df.status == 'Ready') &
            (self.resource_df.working.apply(lambda w: w is None).astype(bool) | (self.resource_df.working == 'training'))
            ]
        group_available_nodes = available_resource_df.groupby('group').name.apply(set).to_dict()
        group_available_nodes = {g: {'free': ns, 'working': set()} for g, ns in group_available_nodes.items()}
        can_run_task_df = self.task_df[self.task_df.assign_result == ASSIGN_RESULT.CAN_RUN].sort_values(['custom_rank', 'first_id']).sort_values('priority', kind='mergesort', ascending=False)
        running_task_df = can_run_task_df[can_run_task_df.queue_status == QUE_STATUS.SCHEDULED]
        group_running_nodes = running_task_df.explode('assigned_nodes').groupby('group').assigned_nodes.apply(set).to_dict()
        for group, g_running_nodes in group_running_nodes.items():
            if group_available_nodes.get(group) is None:
                group_available_nodes[group] = {'free': set(), 'working': set()}
            t = group_available_nodes[group]['free'] & g_running_nodes
            group_available_nodes[group]['free'] -= t
            group_available_nodes[group]['working'] |= t
        can_run_task_ids = set()
        task_id_assigned_nodes = {}
        for _, task in can_run_task_df.iterrows():
            if task.queue_status == QUE_STATUS.SCHEDULED:
                this_task_assigned_nodes = group_available_nodes[task.group]['working'] & set(task.assigned_nodes)
                if len(this_task_assigned_nodes) == task.nodes:
                    can_run_task_ids.add(task.id)
                    group_available_nodes[task.group]['working'] -= this_task_assigned_nodes
                    continue
                else:
                    group_available_nodes[task.group]['working'] -= this_task_assigned_nodes
                    group_available_nodes[task.group]['free'] |= this_task_assigned_nodes
            if len(group_available_nodes[task.group]['free']) >= task.nodes:
                this_task_assigned_nodes = random.sample(group_available_nodes[task.group]['free'], task.nodes)
                group_available_nodes[task.group]['free'] -= set(this_task_assigned_nodes)
            elif len(group_available_nodes[task.group]['free']) + len(group_available_nodes[task.group]['working']) >= task.nodes:
                this_task_assigned_nodes = list(group_available_nodes[task.group]['free'])
                group_available_nodes[task.group]['free'] = set()
                rest_nodes = random.sample(group_available_nodes[task.group]['working'], task.nodes - len(group_available_nodes[task.group]['free']))
                group_available_nodes[task.group]['working'] -= set(rest_nodes)
                this_task_assigned_nodes += rest_nodes
            else:
                continue
            if task.queue_status == QUE_STATUS.QUEUED:
                task_id_assigned_nodes[task.id] = this_task_assigned_nodes
                can_run_task_ids.add(task.id)
        node_resource = {
            n: {
                "cpu": max(c - self.reserved_cpu, 0),
                "assigned_gpus": [i for i in range(g)],
                "memory": max(m - self.reserved_memory, 0)
            } for n, c, g, m in
            zip(available_resource_df.name, available_resource_df.cpu, available_resource_df.gpu_num, available_resource_df.memory)
        }
        task_id_assigned_cpu = {tid: [node_resource[n]['cpu'] for n in ns] for tid, ns in task_id_assigned_nodes.items()}
        task_id_assigned_gpu = {tid: [node_resource[n]['assigned_gpus'] for n in ns] for tid, ns in task_id_assigned_nodes.items()}
        task_id_assigned_memory = {tid: [node_resource[n]['memory'] for n in ns] for tid, ns in task_id_assigned_nodes.items()}
        self.task_df.loc[self.task_df.id.isin(can_run_task_ids), 'assign_result'] = ASSIGN_RESULT.CAN_RUN
        self.task_df.update(pd.Series(task_id_assigned_nodes, name='assigned_nodes', dtype=object))
        self.task_df.update(pd.Series(task_id_assigned_cpu, name='cpu', dtype=object))
        self.task_df.update(pd.Series(task_id_assigned_gpu, name='assigned_gpus', dtype=object))
        self.task_df.update(pd.Series(task_id_assigned_memory, name='memory', dtype=object))
        self.task_df.loc[(self.task_df.assign_result == ASSIGN_RESULT.CAN_RUN) & (self.task_df.queue_status == QUE_STATUS.QUEUED), 'match_result'] = MATCH_RESULT.STARTUP
        self.task_df.loc[(self.task_df.assign_result == ASSIGN_RESULT.CAN_RUN) & (self.task_df.queue_status == QUE_STATUS.SCHEDULED), 'match_result'] = MATCH_RESULT.KEEP_RUNNING
        self.task_df.loc[(~(self.task_df.assign_result == ASSIGN_RESULT.CAN_RUN)) & (self.task_df.queue_status == QUE_STATUS.QUEUED), 'match_result'] = MATCH_RESULT.DO_NOTHING
        self.task_df.loc[(~(self.task_df.assign_result == ASSIGN_RESULT.CAN_RUN)) & (self.task_df.queue_status == QUE_STATUS.SCHEDULED), 'match_result'] = MATCH_RESULT.SUSPEND
