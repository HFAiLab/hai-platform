import uuid
from abc import ABC
from typing import Tuple, Optional

import ujson

from base_model.base_task import BaseTask, ITaskImpl
from server_model.selector import TrainingTaskSelector
from server_model.user_data import update_user_last_activity
from conf.flags import QUE_STATUS, SUSPEND_CODE, TASK_FLAG, TASK_PRIORITY
from db import MarsDB


class DbOperationImpl(ITaskImpl, ABC):
    def create(self, *args, **kwargs) -> Optional[BaseTask]:
        db_conn = kwargs.get('db_conn', MarsDB())
        task = self.task
        # 在插入的时候，构建
        if task.chain_id is None:
            task.chain_id = str(uuid.uuid4())
        table = 'unfinished_task_ng' if task.queue_status != QUE_STATUS.FINISHED else 'task_ng'
        sql = f'''
            insert into "{table}" (
                "nb_name", "user_name", "code_file", "workspace", "group", "nodes", "assigned_nodes", "restart_count", 
                "whole_life_state", "first_id", "backend", "task_type", "queue_status", "notes", "priority", "chain_id", 
                "mount_code", "config_json", "worker_status", "suspend_code"
                )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            returning "id"
        '''
        try:
            params = (
                task.nb_name, task.user_name, task.code_file, task.workspace, task.group, task.nodes,
                task.assigned_nodes, task.restart_count, task.whole_life_state, task.first_id, task.backend,
                task.task_type, task.queue_status, task.notes, task.priority, task.chain_id, task.mount_code, ujson.dumps(task.config_json),
                task.worker_status, task.suspend_code
            )
            res = db_conn.execute(sql, params)
            task_id = res.fetchall()[0][0]
            task = TrainingTaskSelector.find_one(self.task.__implement_cls__, id=task_id)
            self.update_user_last_activity()
            return task
        except Exception as exp:
            print(exp, flush=True)
            raise exp

    def create_error_info(self, failed_msg, *args, **kwargs):  # 这个函数只在manager中调用，不会用到aio_db
        task = self.task
        sql = f'''
            insert into "task_error_info" ("id", "error_info") 
            values ({task.id}, %s)
            on conflict do nothing
        '''
        MarsDB().execute(sql, (failed_msg, ))

    def update_config_json_by_path(self, path: Tuple[str], value, *args, **kwargs):
        path = ','.join(path)
        sql = f'''
            update "task_ng" set "config_json" = jsonb_set("config_json", '{{ {path} }}', %s)
            where "id" = {self.task.id}
        '''
        MarsDB().execute(sql, (ujson.dumps(value), ))
        self.update_user_last_activity()

    def update(self, fields: Tuple[str, ...], values: Tuple, *args, **kwargs):
        """

        @param fields:
        @param values:
        @return:
        """
        sql_list = []
        params_list = []
        for i in range(len(fields)):
            # config_json 的改动要特殊处理
            if fields[i] == 'config_json':
                config_json_sql = f"""
                    update "task_ng" 
                    set "config_json" = "config_json" || %s
                    where "id" = {self.task.id}
                """
                sql, params = config_json_sql, (ujson.dumps(values[i]),)
            else:
                _sql = f'''
                    update "task_ng" set "{fields[i]}" = %s where "id" = {self.task.id}
                '''
                sql, params = _sql, (values[i],)
            sql_list.append(sql)
            params_list.append(params)
        db_conn = kwargs.get('db_conn')
        if db_conn:
            for sql, params in zip(sql_list, params_list):
                db_conn.execute(sql, params)
        else:
            MarsDB().execute_many(sql_list, params_list)
        for i in range(len(fields)):
            if fields[i] == 'config_json':
                self.task.config_json = {**self.task.config_json, **values[i]}
            else:
                self.task.__setattr__(fields[i], values[i])
        self.update_user_last_activity()

    def update_user_last_activity(self):
        update_user_last_activity(self.task.user_name)

    def resume(self, *args, **kwargs) -> Optional[BaseTask]:
        task = self.task
        db_conn = kwargs.get('db_conn', MarsDB())
        sql = f'''
                    insert into "unfinished_task_ng" (
                        "nb_name", "user_name", "code_file", "workspace", "group", "nodes", "assigned_nodes", "restart_count", 
                        "whole_life_state", "first_id", "backend", "task_type", "queue_status", "notes", "priority", "chain_id", 
                        "mount_code", "config_json", "worker_status", "suspend_code"
                        )
                    (
                    select 
                        "nb_name", "user_name", "code_file", "workspace", "group", "nodes", "assigned_nodes", "restart_count" + 1, 
                        "whole_life_state", "first_id", "backend", "task_type", %s, "notes", %s, "chain_id", 
                        "mount_code", "config_json", %s, ("suspend_code" & %s) + %s
                    from "task_ng" where "id" = %s
                    )
                    returning "id"
                '''
        try:
            params = (
                QUE_STATUS.QUEUED,
                task.config_json.get('schema', {}).get('priority', task.priority) if task.priority != TASK_PRIORITY.AUTO.value else TASK_PRIORITY.AUTO.value,
                "queued",
                TASK_FLAG.STAR,
                SUSPEND_CODE.NO_SUSPEND,
                task.id
            )
            res = db_conn.execute(sql, params)
            task_id = res.fetchall()[0][0]
            task = TrainingTaskSelector.find_one(self.task.__implement_cls__, id=task_id)
            self.update_user_last_activity()
            return task
        except Exception as exp:
            print(exp, flush=True)
            raise exp

    def set_restart_log(self, rule, reason, result, *args, **kwargs):
        try:
            MarsDB().execute("""
            insert into "task_restart_log" ("task_id", "rule", "reason", "result")
            values (%s, %s, %s, %s)
            """, (self.task.id, rule, reason, result))
        except Exception as e:
            if "already exists" in str(e):
                return {
                    'success': 0,
                    'msg': f'已经设置了 {rule} 的 restart_log'
                }
            else:
                return {
                    'success': 0,
                    'msg': '设置 restart_log 失败'
                }
        return {
                    'success': 1,
                    'msg': '设置 restart_log 成功'
                }
