import uuid
from abc import ABC
from typing import Tuple, Optional

import ujson

from base_model.base_task import BaseTask, ITaskImpl
from conf.flags import QUE_STATUS, TASK_FLAG, SUSPEND_CODE, TASK_PRIORITY
from server_model.selector import AioTrainingTaskSelector
from server_model.user_data import update_user_last_activity
from db import MarsDB


class AioDbOperationImpl(ITaskImpl, ABC):
    async def create(self, *args, **kwargs) -> Optional[BaseTask]:
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
            res = await MarsDB().a_execute(sql, params, remote_apply=kwargs.get('remote_apply', False))
            task_id = res.fetchall()[0][0]
            task = await AioTrainingTaskSelector.find_one(self.task.__implement_cls__, id=task_id)
            await self.update_user_last_activity()
            return task
        except Exception as exp:
            print(exp, flush=True)
            raise exp

    async def update(self, fields: Tuple[str, ...], values: Tuple, *args, **kwargs):
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
                config_json_args = (ujson.dumps(values[i]),)
                sql_list.append(config_json_sql)
                params_list.append(config_json_args)
            else:
                sql = f'''
                    update "task_ng" set "{fields[i]}" = %s where "id" = {self.task.id}
                '''
                sql_list.append(sql)
                params_list.append((values[i],))
        await MarsDB().a_execute_many(sql_list=sql_list, params_list=params_list, remote_apply=kwargs.get('remote_apply', False))
        for i in range(len(fields)):
            if fields[i] == 'config_json':
                self.task.config_json = {**self.task.config_json, **values[i]}
            else:
                self.task.__setattr__(fields[i], values[i])
        await self.update_user_last_activity()

    async def tag_task(self, tag: str, *args, **kwargs):
        a_db_conn = kwargs.get('a_db_conn')
        sql = f'''
            insert into "task_tag" ("chain_id", "user_name", "tag")
            values (%s, %s, %s)
            on conflict do nothing;
        '''
        db_execute = MarsDB().a_execute if a_db_conn is None else a_db_conn.execute
        await db_execute(sql, (self.task.chain_id, self.task.user_name, tag), remote_apply=kwargs.get('remote_apply', False))
        await self.update_user_last_activity()

    async def untag_task(self, tag: str, *args, **kwargs):
        sql = f'''
            delete from "task_tag"
            where "chain_id" = %s and "tag" = %s
        '''
        await MarsDB().a_execute(sql, (self.task.chain_id, tag), remote_apply=kwargs.get('remote_apply', False))
        # 如果是删除共享标记 tag, 主动指定更新共享任务时间戳, 否则删除了 tag 会被判定为非共享任务而不去更新时间戳
        await self.update_user_last_activity(from_shared_task=True if tag.startswith('_shared_in') else None)

    async def update_tags(self):
        sql = f'''
            select array_agg(tag) from "task_tag"
            where "chain_id" = %s and "user_name" = %s
        '''
        result = await MarsDB().a_execute(sql, (self.task.chain_id, self.task.user_name))
        result = result.fetchone()[0]
        self.task.tags = list(result) if result is not None else []

    async def map_task_artifact(self, artifact_name: str, artifact_version: str, direction: str, *args, **kwargs):
        if artifact_name == '':
            return 'artifact_name 不能为空'
        if direction not in ['input', 'output']:
            return 'direction 必须为input/output'
        # shared task只能map shared artifact，私有task只能map 私有artifact
        is_shared_task = any(tag.startswith('_shared_in') for tag in self.task.tags)
        shared_group = self.task.user.shared_group if is_shared_task else self.task.user_name
        artifacts_info = (await MarsDB().a_execute("""
        select "shared_group" from "user_artifact"
        where "name" = %s and "version" = %s and ("user_name" = %s or "shared_group" = %s)
        """, (artifact_name, artifact_version, self.task.user_name, shared_group))).fetchall()
        if len(artifacts_info) < 1:
            return f'找不到artifact {artifact_name}:{artifact_version}, 任务{self.task.job_info}类型：{"共享" if is_shared_task else "私有"}'
        # 记录artifact shared_group:name:version
        artifact_info = f'{artifacts_info[0].shared_group}:{artifact_name}:{artifact_version}'
        artifact_info_field = 'in_artifact' if direction == 'input' else 'out_artifact'
        task_ids = {self.task.id, self.task.first_id}
        sql = f'''
            insert into "task_artifact_mapping" ("user_name", "chain_id", "nb_name", "task_ids", "{artifact_info_field}", "shared_group")
            values ('{self.task.user_name}', '{self.task.chain_id}', '{self.task.nb_name}', array {list(task_ids)}, '{artifact_info}', '{shared_group}')
            on conflict ("chain_id") do update set "{artifact_info_field}" = excluded."{artifact_info_field}",
                                                   "task_ids" = excluded."task_ids"
        '''
        await MarsDB().a_execute(sql, remote_apply=kwargs.get('remote_apply', False))
        await self.update_user_last_activity()

    async def unmap_task_artifact(self, direction: str, *args, **kwargs):
        if direction not in ['input', 'output', 'all']:
            return 'direction 必须为input/output/all'
        if direction == 'all':
            _sql = f'''
                delete from "task_artifact_mapping"
                where "user_name" = '{self.task.user_name}' and "chain_id" = '{self.task.chain_id}'
            '''
        else:
            _sql = f'''
                update "task_artifact_mapping"
                set "{'in_artifact' if direction == 'input' else 'out_artifact'}" = ''
                where "user_name" = '{self.task.user_name}' and "chain_id" = '{self.task.chain_id}'
            '''
        await MarsDB().a_execute(_sql, remote_apply=kwargs.get('remote_apply', False))
        await self.update_user_last_activity()

    async def get_task_artifact(self, *args, **kwargs):
        result = (await MarsDB().a_execute(f"""
            select "user_name", "chain_id", "nb_name", "task_ids", "in_artifact", "out_artifact" from "task_artifact_mapping"
            where "chain_id" = '{self.task.chain_id}' """)).fetchall()
        if len(result) == 0:
            return f'no artifact mapped for task {self.task.job_info}'
        row = result[0]._asdict()
        for index in ['in_artifact', 'out_artifact']:
            if row[index]:
                row[index] = ':'.join(row[index].split(':')[1:])
        return row

    async def update_user_last_activity(self, from_shared_task=None):
        if from_shared_task is None:
            await self.update_tags()
            from_shared_task = any(tag.startswith('_shared_in') for tag in self.task.tags)
        update_user_last_activity(self.task.user_name, from_shared_task=from_shared_task)

    async def aio_update_config_json_by_path(self, path, value, *args, **kwargs):
        path = ','.join(path)
        sql = f'''
            update "task_ng" set "config_json" = jsonb_set("config_json", '{{ {path} }}', %s)
            where "id" = {self.task.id}
        '''
        await MarsDB().a_execute(sql, (ujson.dumps(value), ))

    async def resume(self, *args, **kwargs) -> Optional[BaseTask]:
        task = self.task
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
            res = await MarsDB().a_execute(sql, params, remote_apply=kwargs.get('remote_apply', False))
            task_id = res.fetchall()[0][0]
            task = await AioTrainingTaskSelector.find_one(self.task.__implement_cls__, id=task_id)
            await self.update_user_last_activity()
            return task
        except Exception as exp:
            print(exp, flush=True)
            raise exp

    async def set_restart_log(self, rule, reason, result, *args, **kwargs):
        try:
            await MarsDB().a_execute("""
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
