

from collections import defaultdict
from itertools import chain

import munch

from conf import MARS_GROUP_FLAG, CONF
from conf.flags import TASK_TYPE, QUE_STATUS, USER_ROLE
from k8s.v1_api import set_node_label
from scheduler.base_model import Assigner, ASSIGN_RESULT
from server_model.user_impl import UserQuota

move_out_node_time_count = {}
# 因为点了挪分组之后，不一定能用上，所以这里加一个 count，暂定 100 次调度都没用到就挪回去，也能增加调度稳定性
MOVE_TIME_COUNT = 100
# 外部用户的独占节点 3 次调度没用到就挪回去, 因为挪完分组都会立刻起任务, 挪的节点一定会用上, 而且容器打断后不会重启
INCREMENT_FOR_EXTERNAL = 34
SHARED_NODE_GROUP = CONF.jupyter.shared_node_group_prefix


class JupyterAssigner(Assigner):

    def __init__(self, **kwargs):
        super(JupyterAssigner, self).__init__(**kwargs)
        self.register_global_config(mig_config={})
        self.cluster_busy_threshold = CONF.jupyter.spot.get('num_free_node_thresholds', {}).get('min_to_run', 150)
        self.max_num_spot_jupyter = CONF.jupyter.spot.get('max_number', 50)
        self.labels_to_set = set()

    def process_schedule(self):
        self.resource_df = self.resource_df[self.resource_df.active]
        all_task_df = self.task_df.copy()
        self.task_df = self.task_df[(self.task_df.task_type == TASK_TYPE.JUPYTER_TASK) & (
            self.task_df.queue_status.isin({QUE_STATUS.QUEUED, QUE_STATUS.SCHEDULED}))].copy()

        # 为 matcher 准备 background task 相关的数据
        running_bg_tasks_df = all_task_df[(all_task_df.task_type == TASK_TYPE.BACKGROUND_TASK) & (all_task_df.queue_status == QUE_STATUS.SCHEDULED)]
        self.extra_data['bg_task_node_set'] = set(chain(*running_bg_tasks_df.assigned_nodes.to_list()))

        # 把独占的任务占用的节点移出集群，防止监控处理这些节点
        node_to_task = self.task_df[(self.task_df.queue_status == QUE_STATUS.SCHEDULED) & (
            ~self.task_df.group.str.startswith(SHARED_NODE_GROUP))].explode('assigned_nodes').groupby(
            'assigned_nodes')[['user_name', 'is_spot_jupyter', 'user_role']].first().to_dict('index')
        in_nodes_df = self.resource_df[self.resource_df.name.isin(node_to_task)]
        labels_to_set_this_time = set()
        for _, row in in_nodes_df.iterrows():
            task = munch.Munch(node_to_task[row['name']])
            prefix = (meta_group + '.') if (meta_group := CONF.jupyter.get('node_meta_group')) else ''
            if task.user_role == USER_ROLE.EXTERNAL:
                prefix += 'external_spot.' if task.is_spot_jupyter else 'external.'
            dedicated_group = f"{prefix}{task.user_name}_dedicated"
            if row.mars_group != dedicated_group:
                labels_to_set_this_time.add((row['name'], dedicated_group, "独占容器占用"))

        # 把独占任务释放的节点还回去
        out_nodes_df = self.resource_df[
            (~self.resource_df.name.isin(node_to_task)) & (self.resource_df.group.str.endswith('_dedicated', na=False))]
        global move_out_node_time_count
        for _, row in out_nodes_df.iterrows():
            increment = INCREMENT_FOR_EXTERNAL if 'external_spot.' in row.mars_group else 1
            if move_out_node_time_count.get(row['name']):
                move_out_node_time_count[row['name']] += increment
            else:
                move_out_node_time_count[row['name']] = increment
        move_out_node_time_count = {k: v for k, v in move_out_node_time_count.items() if k in out_nodes_df.name.to_list()}
        for _, row in out_nodes_df[out_nodes_df.name.apply(lambda n: move_out_node_time_count[n] >= MOVE_TIME_COUNT)].iterrows():
            by_who = row.group.replace('_dedicated', '')
            labels_to_set_this_time.add((row['name'], row.origin_group, f"{by_who} 的独占容器结束"))

        # resource_df 有延迟, 避免多次操作 node label
        for node_name, new_label, msg in (labels_to_set_this_time - self.labels_to_set):
            self.info((f"把 {node_name} 挪到 {new_label} 中 ({msg})"
                       f"{set_node_label(node_name, MARS_GROUP_FLAG, new_label)}"))
        self.labels_to_set = labels_to_set_this_time

        self.task_df.assign_result = ASSIGN_RESULT.CAN_RUN

        # 外部用户 Spot 独占容器相关的处理逻辑
        spot_task_mask = (self.task_df.user_role == USER_ROLE.EXTERNAL) & (self.task_df.is_spot_jupyter)
        # 控制总数, 由于集群信息有延迟, 总数超量时 server 端可能没有阻断所有请求, 优先停止掉在排队的和新创建的
        spot_tasks = self.task_df[spot_task_mask].sort_values(by=['queue_status', 'created_seconds'], ascending=False)
        self.task_df.loc[spot_tasks.iloc[self.max_num_spot_jupyter:].index, ['assign_result', 'scheduler_msg']] = \
            [ASSIGN_RESULT.CAN_NOT_RUN, '外部用户独占开发容器总数超量']
        # 集群空闲节点数低于阈值或有排队任务节点需求量超过阈值, 回收所有外部用户的独占开发容器
        spot_nodes = self.resource_df[self.resource_df.mars_group.str.contains('external_spot.') & 1]  # &1: filter None
        if len(spot_nodes) > 0 and spot_task_mask.any():
            num_free_nodes = len(self.resource_df[
                (self.resource_df.status == 'Ready') &
                (self.resource_df.group == CONF.scheduler.default_group) &
                (self.resource_df.working.isnull() | (self.resource_df.working_user_role == USER_ROLE.EXTERNAL))
            ])
            big_task_queued = (
                    (all_task_df.queue_status == QUE_STATUS.QUEUED) &
                    (all_task_df.user_role == USER_ROLE.INTERNAL) &
                    (all_task_df.created_seconds > 10) &
                    (all_task_df.nodes > self.cluster_busy_threshold)
            ).any()
            if num_free_nodes < self.cluster_busy_threshold or big_task_queued:
                self.info((f'触发外部用户独占节点回收: '
                           f'集群当前空闲节点小于阈值 ({num_free_nodes} < {self.cluster_busy_threshold}) '
                           f'或有多节点任务 {big_task_queued}'))
                self.task_df.loc[spot_task_mask, ['assign_result', 'scheduler_msg']] = \
                    [ASSIGN_RESULT.CAN_NOT_RUN, '外部独占节点被全部回收']

        # check jupyter_quota
        jupyter_quota = defaultdict(lambda: defaultdict(lambda: {'cpu': 0, 'memory': 0, 'quota': 0}))
        for user_name, df in self.user_df[self.user_df.resource.str.startswith('jupyter:')].groupby('user_name'):
            jupyter_quota[user_name] = UserQuota.df_to_jupyter_quota(df.groupby('resource').max())
        self.task_df.sort_index(inplace=True)
        for ind, row in self.task_df.iterrows():
            quota = jupyter_quota[row.user_name].get(row.group, {'cpu': 0, 'memory': 0, 'quota': 0})
            quota_check = True
            schema = row.config_json.get('schema', {})
            cpu, memory = schema.get('resource', {}).get('cpu', -1), schema.get('resource', {}).get('memory', -1)
            if cpu > quota['cpu']:
                quota_check = False
            if memory > quota['memory']:
                quota_check = False
            if quota['quota'] <= 0:
                quota_check = False
            if quota_check:
                jupyter_quota[row.user_name][row.group]['memory'] -= memory
                jupyter_quota[row.user_name][row.group]['quota'] -= 1
            else:
                self.task_df.loc[self.task_df.index == ind, ['assign_result', 'scheduler_msg']] = [
                    ASSIGN_RESULT.CAN_NOT_RUN, "quota 不足"]
        try:
            mig_mapping = self.global_config.get('mig_config', {})
            for ind, row in self.task_df.iterrows():
                if row.group.startswith(CONF.jupyter.mig_node_group_prefix):
                    if mig_mapping.get(row.user_name):
                        self.task_df.loc[self.task_df.index == ind, 'assigned_gpus'] = [mig_mapping[row.user_name]]
                    else:
                        self.task_df.loc[self.task_df.index == ind, ['assign_result', 'scheduler_msg']] = [ASSIGN_RESULT.CAN_NOT_RUN, '没有 mig 信息，不调度']
        except:
            self.task_df.loc[self.task_df.group.str.startswith(CONF.jupyter.mig_node_group_prefix),
                ['assign_result', 'scheduler_msg']] = [ASSIGN_RESULT.CAN_NOT_RUN, '没有 mig 信息，不调度']
