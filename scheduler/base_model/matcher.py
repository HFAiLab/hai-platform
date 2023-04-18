import pandas as pd
import ujson
from sqlalchemy.engine import Connection

from conf import CONF
from conf.flags import STOP_CODE, QUE_STATUS
from db import redis_conn, MarsDB
from .base_processor import BaseProcessor
from .base_types import ASSIGN_RESULT, MATCH_RESULT
from roman_parliament import register_parliament, add_archive_for_senators
from roman_parliament.archive_triggers.launcher_task_trigger import LAUNCHER_COUNT
from logm import logger


def modify_task_df_safely(task_df, task_id, **kwargs):
    """
    修改 task_df 的方法，用来撮合完成时分配资源，用来解决目标字段是 list 时无法正确 .loc 的问题
    task_id 就是 index
    kwargs 为 {'字段名': '预期字段值'}
    """
    for k, v in kwargs.items():
        tmp_series = task_df[k].copy()
        tmp_series[task_id] = v
        task_df[k] = tmp_series
    return task_df


class InsertTaskTimeout(Exception):
    """上次插入任务超时了"""
    pass


class Matcher(BaseProcessor):
    """
    Matcher 模块，上游为若干 Assigner，负责处理 assign 结果，撮合任务和资源，发送起停信号
    """
    re_signal_where = ''

    def __init__(self, **kwargs):
        self.tasks_to_start_df = pd.DataFrame(columns=['id', 'task_type'])
        self.tasks_to_stop_df = pd.DataFrame(columns=['id', 'task_type'])
        self.tasks_to_suspend_df = pd.DataFrame(columns=['id', 'task_type'])
        super(Matcher, self).__init__(**kwargs)

    def user_tick_process(self):
        # match
        self.process_match()
        # apply_db & send_signal
        if self.valid:
            try:
                with MarsDB() as conn:
                    self.apply_db(conn)
                self.send_signal()
            except InsertTaskTimeout as e:
                self.info(str(e))
            except Exception as e:
                logger.exception(e)
                self.error(f'这次 match 有问题，rollback db 等下次, {e}')

    def start(self):
        register_parliament()
        super(Matcher, self).start()

    def process_match(self):
        raise NotImplementedError

    def apply_db(self, conn: Connection):
        """
        这里做要操作数据库的操作
        """
        inactive_user_df = self.user_df[~self.user_df.active]
        inactive_users = inactive_user_df.user_name if len(inactive_user_df) else []
        self.task_df.loc[self.task_df.user_name.isin(inactive_users), ['assign_result', 'match_result', 'scheduler_msg']] = [ASSIGN_RESULT.CAN_NOT_RUN, MATCH_RESULT.STOP, '用户账号不活跃了']
        self.tasks_to_start_df = self.task_df[self.task_df.match_result == MATCH_RESULT.STARTUP]
        self.tasks_to_suspend_df = self.task_df[self.task_df.match_result == MATCH_RESULT.SUSPEND]
        self.tasks_to_stop_df = self.task_df[self.task_df.match_result == MATCH_RESULT.STOP]
        if not any([len(self.tasks_to_start_df), len(self.tasks_to_suspend_df), len(self.tasks_to_stop_df)]):
            return
        self.start_db_task(conn)
        self.stop_db_task(conn)
        self.suspend_db_task(conn)

    def start_db_task(self, conn: Connection):
        for _, row in self.tasks_to_start_df.sort_index().iterrows():
            sql, params = f"""
                update "unfinished_task_ng" 
                set  "queue_status" = %s, "assigned_nodes" = %s, "config_json" = "config_json" || %s
                where "id" = %s and "queue_status" = %s
                returning "unfinished_task_ng"."id"
            """, (
                QUE_STATUS.SCHEDULED,
                row.assigned_nodes,
                ujson.dumps({
                    'assigned_resource': {
                        'memory': row.memory,
                        'cpu': row.cpu,
                        'assigned_gpus': row.assigned_gpus,
                        'assigned_numa': row.assigned_numa
                    }
                }),
                row.id,
                QUE_STATUS.QUEUED
            )
            # self.info(f'[SQL] ' + sql.replace("\n", " ").strip() + f' {params}')
            res = conn.execute(sql, params).fetchall()
            if len(res) == 0:
                raise InsertTaskTimeout('这个任务已经不是排队状态了，可能用户停止了 / 上一个 tick 已经调度到这个任务了')

    def stop_db_task(self, conn: Connection):
        for _, task in self.tasks_to_stop_df.iterrows():
            conn.execute(f'''
            update "unfinished_task_ng" set "queue_status" = %s where "id" = %s and "queue_status" = %s
            ''', (QUE_STATUS.FINISHED, task.id, QUE_STATUS.QUEUED))
            redis_conn.set(f'ban:{task.user_name}:{task.nb_name}:{task.chain_id}', 1)  # 防止重启
            redis_conn.lpush(f'{CONF.manager.stop_channel}:suspend:{task.id}', ujson.dumps({'stop_code': STOP_CODE.STOP}))

    def suspend_db_task(self, conn: Connection):
        pass

    def send_signal(self):
        """
        发送起停信号
        """
        try:
            for tid in self.tasks_to_suspend_df.id.to_list():
                redis_conn.lpush(
                    f'{CONF.manager.stop_channel}:suspend:{tid}',
                    ujson.dumps({'stop_code': STOP_CODE.INTERRUPT})
                )
        except Exception as e:
            logger.exception(e)
        try:
            start_task_id_list = {
                task_id: task_id % LAUNCHER_COUNT
                for task_id in self.tasks_to_start_df.id.to_list()
            }
            if self.seq % (CONF.scheduler.re_signal * 1000) == 0 and len(self.re_signal_where) > 0:
                not_started_tasks = {
                    k: v
                    for k, v in MarsDB().execute(f"""
                    select
                        "unfinished_task_ng"."id", extract(epoch from (current_timestamp - "unfinished_task_ng"."begin_at")) as "created_seconds"
                    from "unfinished_task_ng"
                    left join "pod_ng" on "pod_ng"."task_id" = "unfinished_task_ng"."id"
                    where
                        "pod_ng"."task_id" is null and
                        "unfinished_task_ng"."queue_status" = '{QUE_STATUS.SCHEDULED}' and
                        "unfinished_task_ng"."begin_at" < current_timestamp - interval '{CONF.scheduler.re_signal} sec' and
                        {self.re_signal_where}
                    """).fetchall()
                }
                for task_id, delay_seconds in not_started_tasks.items():
                    start_task_id_list[task_id] = ((task_id % LAUNCHER_COUNT) + int(delay_seconds / CONF.scheduler.re_signal)) % LAUNCHER_COUNT
                    if delay_seconds > CONF.scheduler.send_fetion:
                        msg = f'任务 {task_id} 过了 {delay_seconds} 秒都没有起来，请检查'
                        self.f_error(msg)
            if len(start_task_id_list) > 0:
                add_archive_for_senators(trigger_name='LauncherTaskTrigger', data=start_task_id_list)
        except Exception as e:
            logger.exception(e)
            logger.error(f'match send_signal exception error, {e}')
