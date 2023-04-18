

"""
这里单独抽出来：
写到 task 类 和 impl 里感觉有点乱
目前修改的需求只存在于 server 端，没有必要抽象出通用方法
scheduler 需要单独有获取 sql 的功能来加速
"""


import ujson
from db import MarsDB
from base_model.base_task import BaseTask
from server_model.user_data import update_user_last_activity


class TaskRuntimeConfig(object):

    def __init__(self, task: BaseTask):
        self.task: BaseTask = task

    def get_insert_sql(self, source, config_json, chain=False, update=False, *args, **kwargs):
        column = 'chain_id' if chain else 'task_id'
        return f"""
        insert into "task_runtime_config" ("{column}", "config_json", "source")
        values (%s, %s, %s)
        on conflict ("{column}", "source") do update set "config_json" = {'"task_runtime_config"."config_json" || ' if update else ''} excluded."config_json";
        """, (self.task.chain_id if chain else self.task.id, ujson.dumps(config_json), source)

    def insert(self, source, config_json, chain=False, update=False, *args, **kwargs):
        with MarsDB() as conn:
            conn.execute(*self.get_insert_sql(source, config_json, chain, update, args, kwargs))
        self._update_user_last_activity()

    def update_by_path(self, source, path, value, *args, **kwargs):
        path = ','.join(path)
        sql = f'''
            update "task_runtime_config" set "config_json" = jsonb_set("config_json", '{{ {path} }}', %s)
            where "task_id" = {self.task.id} and "source" = %s
        '''
        with MarsDB() as conn:
            conn.execute(sql, (ujson.dumps(value), source))
        self._update_user_last_activity()

    async def a_insert(self, source, config_json, chain=False, update=False, *args, **kwargs):
        async with MarsDB() as conn:
            await conn.execute(*self.get_insert_sql(source, config_json, chain, update, args, kwargs))
        self._update_user_last_activity()

    def get_delete_sql(self, source, chain=False, *args, **kwargs):
        column = 'chain_id' if chain else 'task_id'
        return f"""
        delete from "task_runtime_config"
        where "{column}" = %s and "source" = %s;
        """, (self.task.chain_id if chain else self.task.id, source)

    def delete(self, source, chain=False, *args, **kwargs):
        with MarsDB() as conn:
            conn.execute(*self.get_delete_sql(source, chain, args, kwargs))
        self._update_user_last_activity()

    async def a_delete(self, source, chain=False, *args, **kwargs):
        async with MarsDB() as conn:
            await conn.execute(*self.get_delete_sql(source, chain, args, kwargs))
        self._update_user_last_activity()

    def _update_user_last_activity(self):
        update_user_last_activity(self.task.user_name)
