

from typing import Tuple

from cached_property import cached_property

from base_model.base_task import BasePod
from server_model.user_data import update_user_last_activity
from conf.flags import EXP_STATUS
from db import MarsDB


class Pod(BasePod):
    def __init__(self, task_id, pod_id, job_id, status, node, role, assigned_gpus, created_at=None, begin_at=None,
                 end_at=None, memory=0, cpu=0, exit_code='nan',
                 **kwargs):
        """

        @param pod_id:
        @param status:
        @param node:
        @param job_id:
        @param xp_id:
        @param role:
        @param started_at:
        @param assigned_gpus:
        @param exit_code:
        """
        super().__init__(task_id, pod_id, job_id, status, node, role, assigned_gpus, created_at, begin_at,
                         end_at, memory, cpu, exit_code, **kwargs)

    def insert(self, *args, **kwargs):
        """
        初始化插入数据
        @return:
        """
        db_conn = kwargs.get('db_conn', MarsDB())
        sql = f'''
            insert into "pod_ng" (
            "task_id", "pod_id", "job_id", "status", "node", "assigned_gpus", "memory", "cpu", "role"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        db_conn.execute(sql, (self.task_id, self.pod_id, self.job_id, self.status, self.node, self.assigned_gpus, self.memory, self.cpu, self.role))
        self.update_user_last_activity()
        return self

    def update(self, fields: Tuple[str, ...], values: Tuple, *args, **kwargs):
        sets = []
        for i in range(len(fields)):
            sets.append(f'"{fields[i]}" = %s')
        sql = f'''
            update "pod_ng" set {','.join(sets)} where "pod_id" = '{self.pod_id}' returning current_timestamp
        '''
        result = MarsDB().execute(sql, values)
        for i in range(len(fields)):  # 等数据库落地了，再改内存
            self.__setattr__(fields[i], values[i])
        self.update_user_last_activity()
        return result

    async def a_update(self, fields: Tuple[str, ...], values: Tuple, *args, **kwargs):
        sets = []
        for i in range(len(fields)):
            sets.append(f'"{fields[i]}" = %s')
        sql = f'''
            update "pod_ng" set {','.join(sets)} where "pod_id" = '{self.pod_id}'
        '''
        await MarsDB().a_execute(sql, values)
        for i in range(len(fields)):
            self.__setattr__(fields[i], values[i])
        self.update_user_last_activity()

    @classmethod
    def where(cls, where, args):
        items = []
        sql = f''' 
            select * from pod_ng where {where};
        '''
        result = MarsDB().execute(sql, args)
        for r in result:
            item = cls(**r)
            items.append(item)
        return items

    @classmethod
    def find_pods(cls, task_id):
        return cls.where('"task_id" = %s order by "job_id"', (task_id, ))

    @classmethod
    def find_pods_by_pod_id(cls, pod_id):
        return cls.where('"pod_id" = %s', (pod_id, ))

    @classmethod
    def find_pods_by_job(cls, job_id):
        return cls.where('"job_id" = %s', (job_id, ))

    @classmethod
    def find_running_pods(cls):
        return cls.where(f'''
            "status" in ({','.join([f"'{s}'" for s in EXP_STATUS.UNFINISHED])})
        ''', ())

    @classmethod
    async def a_where(cls, where, args):
        items = []
        sql = f''' 
            select * from pod_ng where {where};
        '''
        result = await MarsDB().a_execute(sql, args)
        for r in result:
            item = cls(**r)
            items.append(item)
        return items

    @classmethod
    async def aio_find_pods(cls, task_id):
        return await cls.a_where('"task_id" = %s order by "job_id"', (task_id, ))

    @classmethod
    async def aio_find_pods_by_pod_id(cls, pod_id):
        return await cls.a_where('"pod_id" = %s', (pod_id, ))

    @classmethod
    def empty_pod(cls, *args, **kwargs):
        node = kwargs.get('node', 'None')
        return cls(
            task_id=0, pod_id='', job_id=0, status='waiting-init', node=node, role='None', assigned_gpus=[]
        )

    @cached_property
    def possible_user_names(self):
        # 创建 pod 时会把 user name 中的下划线转换为横线, 只能还原出可能的用户名
        res = ['-'.join(self.pod_id.split('-')[:-2])]
        if '-' in res[0]:
            res.append(res[0].replace('-', '_'))
        return res

    @property
    def from_shared_task(self):
        sql = """
            select 1 from "task_ng" left join "task_tag" on "task_ng"."chain_id" = "task_tag"."chain_id"
            where id = %s and "task_tag"."tag" like '_shared_in_%'
        """
        res = MarsDB().execute(sql, (self.task_id, ))
        return len(res.fetchall()) > 0

    def update_user_last_activity(self):
        # 从 pod name 还原不出准确的 user name, 把两种可能都更新一下, 开销比查数据库拿准确的 user name 要小很多
        for user_name in self.possible_user_names:
            update_user_last_activity(user_name, from_shared_task=self.from_shared_task)
