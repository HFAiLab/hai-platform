

import ujson
import datetime
import pandas as pd

from db import redis_conn, MarsDB
from base_model.base_task import BaseTask
from conf.flags import QUE_STATUS, TASK_PRIORITY
from server_model.task_runtime_config import TaskRuntimeConfig
from server_model.task_impl import DbOperationImpl
from scheduler.base_model import Subscriber, ASSIGN_RESULT, MATCH_RESULT, TickData


priority_map = TASK_PRIORITY.value_key_map()


class MatcherLogger(Subscriber):
    """
    专门负责打印 matcher 的日志
    """
    def __init__(self, **kwargs):
        super(MatcherLogger, self).__init__(**kwargs)
        self.last_tick_data = TickData()
        self.runtime_config = TaskRuntimeConfig(task=BaseTask())

    def process_subscribe(self):
        self.set_tick_data(self.waiting_for_upstream_data())
        if self.valid:
            self.log_task()
            if 'gpu' in self.name:
                self.record_rule()
            self.last_tick_data = self.tick_data

    def log_task(self):
        """
        打印日志，发送 fetion 消息等
        """

        def fmt_log(rr):
            return (
                    f'[TASK_INFO] ' +
                    '{id}, {user_name}, {sliced_chain_id}, {task_type}, {priority_name}, '
                    '{assign_result}, {match_result}, {scheduler_msg}'.format(
                        priority_name=priority_map.get(rr.priority, 'AUTO'),
                        sliced_chain_id=rr.chain_id[:8],
                        **rr)
            )

        last_tick_data = self.last_tick_data
        tick_data = self.tick_data
        tick_data.task_df['task_id'] = tick_data.task_df.id
        last_tick_data.task_df['task_id'] = last_tick_data.task_df.id
        tmp_df = pd.merge(tick_data.task_df, last_tick_data.task_df, how='left', on='task_id', suffixes=('', '_last'))
        res_to_print = tmp_df[(tmp_df.assign_result != tmp_df.assign_result_last) | (tmp_df.match_result != tmp_df.match_result_last) | (tmp_df.scheduler_msg != tmp_df.scheduler_msg_last)]
        for _, row in res_to_print.iterrows():
            if row.match_result in {MATCH_RESULT.STARTUP, MATCH_RESULT.SUSPEND, MATCH_RESULT.STOP}:
                self.info(fmt_log(row))
            redis_conn.append(f'lifecycle:{row.id}:scheduler', f'{ujson.dumps([str(datetime.datetime.fromtimestamp(tick_data.seq / 1000)), tick_data.seq, row.assign_result, row.match_result, row.scheduler_msg], ensure_ascii=False)}\n')
            redis_conn.expire(f'lifecycle:{row.id}:scheduler', 60 * 60 * 24 * 30)
            if row.assign_result == ASSIGN_RESULT.NODE_ERROR and row.queue_status != QUE_STATUS.QUEUED:
                if 'NotReady' in tick_data.resource_df[tick_data.resource_df.name.isin(row.assigned_nodes)].status.to_list():
                    redis_conn.lpush('node_error_task_channel', row.id)
                self.warning(fmt_log(row))
                task = BaseTask(**row)
                task.re_impl(DbOperationImpl)
                self.f_warning(row.scheduler_msg, task=BaseTask(**row))
                task.set_restart_log(rule='节点异常', reason=row.scheduler_msg, result='智能重启成功')
        # 这里打印调度认为结束了的任务
        for _, row in last_tick_data.task_df[~last_tick_data.task_df.index.isin(tick_data.task_df.index)].iterrows():
            row.scheduler_msg = "任务结束了"
            self.info(fmt_log(row))

    def record_rule(self):
        current_rule = [
            (tid, tr)
            for tid, tcj, tr in
            zip(self.tick_data.task_df.id, self.tick_data.task_df.runtime_config_json, self.tick_data.task_df.match_rank.apply(lambda m: (m >> 19) & 15 if m else -1))
            if tcj.get('scheduler_assign_rule', -1) != tr
        ]
        sql = ''
        params = ()
        for task_id, rule in current_rule:
            self.runtime_config.task.id = task_id
            s, p = self.runtime_config.get_insert_sql('scheduler_assign_rule', rule)
            sql += s
            params += p
        if sql:
            MarsDB().execute(sql, params)
