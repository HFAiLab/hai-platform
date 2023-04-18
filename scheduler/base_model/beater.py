

import datetime
import asyncio
import time
import ujson
# 这个不能删除
import pandas as pd

from . import get_dfs
from .base_processor import BaseProcessor
from db import MarsDB
from server_model.user_data import initialize_user_data_roaming
from base_model.base_task import BaseTask
from server_model.task_runtime_config import TaskRuntimeConfig
from roman_parliament import register_parliament


class Beater(BaseProcessor):
    """
    Beater 负责每隔 interval 的毫秒数，就处理一次数据发出来
    """

    def __init__(self, *, get_dfs_module=get_dfs, interval: int, **kwargs):
        self.interval = interval
        # 还在预热阶段
        self.warmup = True
        self._loop = None
        self.get_dfs_module = get_dfs_module
        super(Beater, self).__init__(**kwargs)
        self.__last_tick_data = self.tick_data
        self.runtime_config = TaskRuntimeConfig(BaseTask())

    def start(self):
        register_parliament()
        initialize_user_data_roaming(tables_to_subscribe=['scheduler_user'], overwrite_enable_roaming=True)
        super(Beater, self).start()

    def user_tick_process(self):
        # 等下一个 tick 到来
        r = datetime.datetime.now().timestamp() * 1000 / self.interval
        next_step = int(r + 1)
        time.sleep(self.interval * (next_step - r) / 1000)
        self.get_tick_data(next_step * self.interval)
        self.feedback_modify()
        if self.valid:
            self.record_priority()
            self.__last_tick_data = self.tick_data

    def get_tick_data(self, seq):
        self.perf_counter()
        self.set_tick_data()
        self.valid = True
        self.seq = seq
        self.task_df = self.get_dfs_module.get_task_df()
        self.user_df = self.get_dfs_module.get_user_df()
        self.resource_df = self.get_dfs_module.get_resource_df(self.loop)
        if len(self.resource_df) == 0:
            self.valid = False
            self.error('没有拿到可用节点，请检查')
        self.update_metric('get_dfs', self.perf_counter())

    def record_priority(self):
        """
        记录最真实的优先级只能在这个地方做掉
        """
        self.perf_counter()
        priority_tick = int(time.time())
        priority_changed_tasks = \
            set((tid, pri) for tid, pri in zip(self.task_df.id, self.task_df.priority)) - \
            set((tid, pri) for tid, pri in zip(self.__last_tick_data.task_df.id, self.__last_tick_data.task_df.priority))
        sql = ''
        params = ()
        for task_id, priority in list(sorted(priority_changed_tasks)):
            task = self.task_df.loc[task_id]
            running_priority = task.runtime_config_json.get('running_priority', [])
            if len(running_priority) == 0 or running_priority[-1]['priority'] != priority:
                running_priority.append({
                    'priority': priority,
                    'timestamp': priority_tick
                })
                self.runtime_config.task.id = task_id
                s, p = self.runtime_config.get_insert_sql('running_priority', running_priority)
                sql += s
                params += p
        if sql:
            MarsDB().execute(sql, params)
        self.update_metric('record_priority', self.perf_counter())

    @property
    def loop(self):
        if self._loop is None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        return self._loop

    def feedback_modify(self):
        """
        根据 FeedBacker 们的修改意见重写 df
        """
        self.perf_counter()
        if self.warmup:
            # 还在预热阶段，直接发送就可以了
            self.valid = False
            self.warmup = False
        else:
            # 非预热阶段
            for upstream in self.upstreams:
                modifier = self.get_upstream_data(upstream)
                # 只要有一个 feedbacker 还没有就绪，就认为还在 warmup
                if not modifier.valid:
                    self.valid = False
                # 这样写感觉不是很直观，但暂时没想到更好的办法，这里需要定义操作，而不是传过来改动后的 df，因为有可能已经过时了
                for exec_str in modifier.extra_data.get('exec_list', []):
                    exec(exec_str)
        self.update_metric('feedback', self.perf_counter())
