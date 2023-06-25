

import copy
from conf.flags import TASK_TYPE, TASK_PRIORITY, CHAIN_STATUS, QUE_STATUS
from scheduler.base_model import Assigner, ASSIGN_RESULT


class FIFOAssigner(Assigner):
    """
    一个简单的 FIFO 分配器示例，从前往后选择尽可能多的可以运行的任务，如果有性能需求，建议另行实现
    """

    def filter_in_quota(self):
        """
        选出 quota 内的任务
        """
        self.task_df = self.task_df[self.task_df.task_type == TASK_TYPE.TRAINING_TASK].copy()
        self.task_df.assign_result = ASSIGN_RESULT.OUT_OF_QUOTA
        raw_user_training_quota = self.user_df[self.user_df.active & (self.user_df.resource == 'node')].groupby(['user_name', 'priority', 'group']).quota.max().to_dict()
        user_training_quota = copy.deepcopy(raw_user_training_quota)
        for _, task in self.task_df.sort_values(['custom_rank', 'first_id']).iterrows():
            if task.priority != TASK_PRIORITY.AUTO.value:
                if user_training_quota.get((task.user_name, task.priority, task.group), 0) - task.nodes >= 0:
                    self.task_df.loc[task.id, 'assign_result'] = ASSIGN_RESULT.NOT_SURE
                    user_training_quota[task.user_name, task.priority, task.group] -= task.nodes
                elif raw_user_training_quota.get((task.user_name, task.priority, task.group), 0) < task.nodes:
                    self.task_df.loc[task.id, 'assign_result'] = ASSIGN_RESULT.QUOTA_EXCEEDED
            else:
                for priority_level in TASK_PRIORITY.all_priorities():
                    if user_training_quota.get((task.user_name, priority_level.value, task.group), 0) - task.nodes >= 0:
                        self.task_df.loc[task.id, ['priority', 'assign_result']] = [priority_level.value, ASSIGN_RESULT.NOT_SURE]
                        user_training_quota[task.user_name, priority_level.value, task.group] -= task.nodes
                        break
                else:
                    if task.nodes > max(quota for (user_name, priority, group), quota in raw_user_training_quota.items() if user_name == task.user_name and group == task.group):
                        self.task_df.loc[task.id, 'assign_result'] = ASSIGN_RESULT.QUOTA_EXCEEDED

    def process_schedule(self):
        self.task_df['chain_status'] = CHAIN_STATUS.RUNNING
        self.task_df.loc[(self.task_df.id == self.task_df.first_id) & (self.task_df.queue_status == QUE_STATUS.QUEUED), 'chain_status'] = CHAIN_STATUS.WAITING_INIT
        self.task_df.loc[(self.task_df.id != self.task_df.first_id) & (self.task_df.queue_status == QUE_STATUS.QUEUED), 'chain_status'] = CHAIN_STATUS.SUSPENDED
        group_free_nodes = self.resource_df[self.resource_df.active & (self.resource_df.status == 'Ready')].groupby('group').name.count().to_dict()
        self.filter_in_quota()
        in_quota_task_df = self.task_df[self.task_df.assign_result == ASSIGN_RESULT.NOT_SURE].sort_values(['custom_rank', 'first_id']).sort_values('priority', kind='mergesort', ascending=False)
        for group, grouped_task_df in in_quota_task_df.groupby('group', sort=False):
            can_run_task_index = grouped_task_df[grouped_task_df.nodes.cumsum() <= group_free_nodes.get(group, 0)].index
            can_not_run_task_index = grouped_task_df[grouped_task_df.nodes.cumsum() > group_free_nodes.get(group, 0)].index
            self.task_df.loc[can_run_task_index, 'assign_result'] = ASSIGN_RESULT.CAN_RUN
            self.task_df.loc[can_not_run_task_index, 'assign_result'] = ASSIGN_RESULT.CAN_NOT_RUN
