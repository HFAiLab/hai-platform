import time
from collections import defaultdict

import ujson
import pandas as pd
import os

from typing import List

from conf.server_flags import TASK_PRIORITY
from db import redis_conn
from scheduler.base_model import Subscriber, TickData, ASSIGN_RESULT
from conf.flags import QUE_STATUS, TASK_TYPE


class ProcessUnitMeta:
    """
    计算单个统计数据的类，需要实现 process_tick_data
    
    新增统计数据的时候新增一个实现
    """
    def __init_subclass__(cls, **kwargs):
        BFFProcessor.process_units.append(cls())
        super().__init_subclass__(**kwargs)

    def process_tick_data(self, tick_data: TickData):
        raise "[ProcessUnitMeta] should overwrite:process_tick_data"

    def get_redis_key_prefix(self):
        return os.environ.get('BFF_REDIS_PREFIX') or 'bff'


class BFFProcessor(Subscriber):
    """
    BFF 的订阅处理器

    last_handle_time: 上次的处理时间，可以在测试的时候节流
    process_units: 处理单元集合
    """
    last_handle_time = time.time()
    process_units: List[ProcessUnitMeta] = list()

    def __init__(self, **kwargs):
        super(BFFProcessor, self).__init__(**kwargs)

    def process_subscribe(self):
        upstream_data_list = []
        for upstream in self.upstreams:
            upstream_data_list.append(self.waiting_for_upstream_data(upstream))

        self.set_tick_data(TickData(
            seq=min(t.seq for t in upstream_data_list),
            resource_df=pd.concat([t.resource_df for t in upstream_data_list]).drop_duplicates(subset=['name']),
            task_df=pd.concat([t.task_df for t in upstream_data_list]).drop_duplicates(subset=['id']),
            user_df=pd.concat([t.user_df for t in upstream_data_list]).drop_duplicates(subset=['user_name', 'resource', 'group', 'priority'])
        ))
        now = time.time()
        # for test:
        # if now - self.last_handle_time < 5:
        #     return;
        self.last_handle_time = now

        for processor in self.process_units:
            processor.process_tick_data(self.tick_data)


class ProcessUnitUserSelfTasks(ProcessUnitMeta):
    """
    用户任务统计

    将用户的任务根据用户名、状态进行分类，统计数量和最长运行时间 (针对运行的任务)
    """
    def process_tick_data(self, tick_data: TickData):
        tasks_json_data = tick_data.task_df[['running_seconds', 'id', 'user_name', 'queue_status']][tick_data.task_df.task_type == TASK_TYPE.TRAINING_TASK] \
            .groupby(by = ['user_name', 'queue_status']) \
            .agg({'id': 'count', 'running_seconds': max}) \
            .reset_index() \
            .rename(columns={'id': 'sum', 'running_seconds': 'max_running_seconds'}) \
            .to_json(orient='records')

        # print('tasks_json_data:', tasks_json_data) # when test
        redis_conn.set(self.get_redis_key(), tasks_json_data)

    def get_redis_key(self):
        return f"{self.get_redis_key_prefix()}:user_self_tasks"


class ProcessUnitUserTopTaskListAll(ProcessUnitMeta):
    """
    缩略展示用户正在运行的任务和排队中的任务
    """
    def process_each_type_data(self, df: pd.DataFrame, order_key):
        df_grouped = df[[order_key, 'user_name', 'chain_id', 'priority', 'chain_status', 'nb_name', 'nodes', 'group', 'queue_status', 'running_seconds', 'created_seconds', 'custom_rank', 'worker_status']] \
            .reset_index()  \
            .groupby('user_name')
        res = {}
        for group, grouped_df in df_grouped:
            res[group] = grouped_df.sort_values(by=order_key, ascending=False).to_dict(orient='records')
        return res

    def process_tick_data(self, tick_data: TickData):
        task_df = tick_data.task_df
        top_tasks_json_data_dict = {
            QUE_STATUS.SCHEDULED: self.process_each_type_data(task_df[(task_df.task_type == TASK_TYPE.TRAINING_TASK) & (task_df.queue_status == QUE_STATUS.SCHEDULED)], 'first_id'),
            QUE_STATUS.QUEUED: self.process_each_type_data(task_df[(task_df.task_type == TASK_TYPE.TRAINING_TASK) & (task_df.queue_status == QUE_STATUS.QUEUED)], 'first_id'),
            'seq': tick_data.seq
        }

        redis_conn.set(self.get_redis_key(), ujson.dumps(top_tasks_json_data_dict))

    def get_redis_key(self):
        return f"{self.get_redis_key_prefix()}:user_top_task_list_all"


class ProcessUnitUserQuota(ProcessUnitMeta):
    """
    用户超出 Quota 的任务统计（永远不会被调度到的那种）
    """
    def process_tick_data(self, tick_data: TickData):
        quota_exceeded_json = tick_data.task_df[(tick_data.task_df.task_type == TASK_TYPE.TRAINING_TASK) & (tick_data.task_df.assign_result == ASSIGN_RESULT.QUOTA_EXCEEDED)]\
            [['running_seconds', 'created_seconds', 'id', 'user_name', 'queue_status', 'nb_name']] \
            .to_json(orient='records')

        redis_conn.set(self.get_redis_key(), quota_exceeded_json)

    def get_redis_key(self):
        return f"{self.get_redis_key_prefix()}:quota_exceeded"


# TODO: 这个可以考虑下次删除了，可以用 ProcessUnitTotalTypedRoleTasks (当前 bff 还有调用，计划近期删除)
class ProcessUnitTotalTasks(ProcessUnitMeta):
    """
    当前所有用户运行和结束的任务汇总，主要用于给外部用户展示，同时避免暴露优先级信息
    """
    def process_tick_data(self, tick_data: TickData):
        training_df = tick_data.task_df[tick_data.task_df.task_type == TASK_TYPE.TRAINING_TASK]
        total_count_dict = {
            'scheduled': len(training_df[training_df.queue_status == QUE_STATUS.SCHEDULED]),
            'queued': len(training_df[training_df.queue_status == QUE_STATUS.QUEUED])
        }
        redis_conn.set(self.get_redis_key(), ujson.dumps(total_count_dict))

    def get_redis_key(self):
        return f"{self.get_redis_key_prefix()}:total_tasks"


# TODO: 后续 bff 使用 ProcessUnitTotalTypedRoleTasks 替换，这段删除
class ProcessUnitTotalTypedTasks(ProcessUnitMeta):
    """
    当前所有用户运行和排队的任务汇总，给内部用户展示使用，区分 group 还有优先级 (消费端可以根据 group 区分是 gpu 还是 cpu)
    """
    def process_each_type_data(self, tasks: pd.DataFrame):
        df = pd.DataFrame(tasks, columns=["priority", "queue_status"])
        df = df.groupby(["priority", "queue_status"]).size().to_frame('count').reset_index()
        df.sort_values(by=['priority', "queue_status"])
        return df

    def process_tick_data(self, tick_data: TickData):
        training_df = tick_data.task_df[tick_data.task_df.task_type == TASK_TYPE.TRAINING_TASK]
        total_count_dict = {}
        df_grouped = training_df.groupby('group')
        for group, grouped_df in df_grouped:
            total_count_dict[group] = self.process_each_type_data(grouped_df).to_dict(orient='records')
        redis_conn.set(self.get_redis_key(), ujson.dumps(total_count_dict))

    def get_redis_key(self):
        return f"{self.get_redis_key_prefix()}:total_typed_tasks"

class ProcessUnitTotalTypedRoleTasks(ProcessUnitMeta):
    """
    当前所有用户运行和排队的任务汇总，可以给下游 bff 进一步消费使用
    """
    def process_each_type_data(self, tasks: pd.DataFrame):
        res_columns = ["priority", "queue_status", "user_role"]
        df = pd.DataFrame(tasks, columns=res_columns).groupby(res_columns).size().to_frame('count').reset_index()
        df.sort_values(by=res_columns)
        return df

    def process_tick_data(self, tick_data: TickData):
        training_df = tick_data.task_df[tick_data.task_df.task_type == TASK_TYPE.TRAINING_TASK]
        total_count_dict = {}
        df_grouped = training_df.groupby('group')
        for group, grouped_df in df_grouped:
            total_count_dict[group] = self.process_each_type_data(grouped_df).to_dict(orient='records')
        redis_conn.set(self.get_redis_key(), ujson.dumps(total_count_dict))

    def get_redis_key(self):
        return f"{self.get_redis_key_prefix()}:total_typed_role_tasks"


class ProcessUnitUsedQuota(ProcessUnitMeta):
    """
    当前所有用户已用节点 quota 的统计
    """
    def process_tick_data(self, tick_data: TickData):
        task_df = tick_data.task_df.copy()
        task_df = task_df[((task_df.queue_status == 'queued') | (task_df.queue_status == 'scheduled')) & (task_df.task_type == 'training')]
        if len(task_df) == 0:
            result = {}
        else:
            priority_map = {v: k for k, v in TASK_PRIORITY.items()}
            task_df['group-priority'] = task_df.group + '-' + task_df.priority.apply(lambda x: priority_map.get(x, 'UNKNOWN'))
            task_df = task_df[['user_name', 'group-priority', 'nodes']]
            res_df = task_df.groupby(['user_name', 'group-priority']).sum()
            result = defaultdict(dict)
            for (user_name, group_priority), nodes in res_df.to_dict()['nodes'].items():
                result[user_name][group_priority] = nodes
        result = {'timestamp': int(time.time()), 'data': result}
        redis_conn.set(self.get_redis_key(), ujson.dumps(result))

    def get_redis_key(self):
        return f"{self.get_redis_key_prefix()}:all_user_used_quota"
