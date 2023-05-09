
from .default import *
from .custom import *

from typing import List

import pandas as pd

from base_model.base_user_modules import IUserQuota
from conf.flags import TASK_PRIORITY, TASK_TYPE, QUE_STATUS
from db import MarsDB
from logm import ExceptionWithoutErrorLog
from server_model.user import User
from server_model.user_data import QuotaTable


class UserQuota(UserQuotaExtras, IUserQuota):
    def __init__(self, user: User):
        super().__init__(user)
        self.user: User = user
        self._quota = None

    async def async_get(self):
        await self.prefetch_quota_df()
        basic_quota = self.basic_quota()
        if self.user.is_internal:
            basic_quota.update({
                'node_quota': self.node_quota,
                'node_quota_limit': self.node_quota_limit,
                'node_quota_limit_extra': self.node_quota_limit_extra,
                'used_node_quota': await self.async_get_used_quota(),
            })
        misc_quota_keys = ['spot_jupyter', 'dedicated_jupyter', 'background_task']
        basic_quota.update({k: int(self.quota(k)) for k in misc_quota_keys})
        return basic_quota

    async def async_set_training_quota(self, group_label, priority_label, quota, expire_time=None):
        if priority_label not in TASK_PRIORITY.keys():      raise ExceptionWithoutErrorLog('priority label 不正确')

        resource = f'node-{group_label}-{priority_label}'
        if not self.user.is_internal:
            # 外部用户需要记录操作日志
            await self.prefetch_quota_df()
            original_quota = self.quota(resource)
            await self.user.aio_db.insert_quota(resource, quota, expire_time=expire_time, remote_apply=False)
            await self.user.aio_db.insert_external_quota_change_log(self.user.user_name, resource, quota, original_quota,
                                                                    expire_time=expire_time, remote_apply=False)
        else:
            if expire_time is not None: raise ExceptionWithoutErrorLog('内部用户不能设置 expire_time')
            await self.user.aio_db.insert_quota(resource, quota, remote_apply=False)

    def __process_quota_df(self, df: pd.DataFrame):
        # 有用户自己的 quota，就不用别的组 quota 了
        user_own_quota = df[df.user_name == self.user.user_name]
        df = pd.concat([df[~df.resource.isin(user_own_quota.resource)], user_own_quota])[['resource', 'quota']]
        return df.groupby('resource').max()

    def __get_quota_df(self):
        df = QuotaTable.df
        df = df[df.user_name.isin(self.user.group_list)]
        return self.__process_quota_df(df)

    async def __async_get_quota_df(self):
        df = await QuotaTable.async_df
        df = df[df.user_name.isin(self.user.group_list)]
        return self.__process_quota_df(df)

    async def create_quota_df(self):
        self._quota = await self.__async_get_quota_df()

    async def prefetch_quota_df(self):
        if self._quota is None:
            await self.create_quota_df()

    async def async_get_used_quota(self):
        used_quota = {k.replace('node-', ''): 0 for k in self.node_quota}
        ts = await MarsDB().a_execute(f"""
        select "group", "priority", "nodes" from "task_ng"
        where
            "user_name" = '{self.user.user_name}' and
            "task_type" = '{TASK_TYPE.TRAINING_TASK}' and 
            "queue_status" in ('{QUE_STATUS.QUEUED}', '{QUE_STATUS.SCHEDULED}')
        """)
        ts = ts.fetchall()
        p_dict = {v: k for k, v in TASK_PRIORITY.items()}
        for task in ts:
            k = f"{task['group']}-{p_dict.get(task['priority'], '')}"
            if k in used_quota:
                used_quota[k] += int(task['nodes'])
        return used_quota

    @property
    def quota_df(self):
        if self._quota is None:
            self._quota = self.__get_quota_df()
        return self._quota

    def quota(self, resource: str):
        if resource in self.quota_df.index:
            return self.quota_df.loc[resource].quota
        return 0

    @property
    def port_quota(self) -> int:
        return int(self.quota(resource='port'))

    @property
    def node_quota(self) -> dict:
        quota_df = self.quota_df
        return quota_df[quota_df.index.str.contains('node-')]['quota'].to_dict()

    @property
    def node_quota_limit(self) -> dict:
        quota_df = self.quota_df
        quota_limit_dict = quota_df[quota_df.index.str.contains('node_limit')]['quota'].to_dict()
        for key in list(quota_limit_dict.keys()):
            resource, group, priority = key.split('-')
            replaced_key = f'node_limit-{group}-{priority}'
            quota_limit_dict[replaced_key] = quota_limit_dict[key] if replaced_key not in quota_limit_dict else min(quota_limit_dict[key], quota_limit_dict[replaced_key])
            if resource != 'node_limit':
                quota_limit_dict.pop(key)
        return quota_limit_dict

    @property
    def node_quota_limit_extra(self) -> dict:
        quota_df = self.quota_df
        return quota_df[quota_df.index.str.contains('node_limit')]['quota'].to_dict()

    @property
    def user_linux_group(self):
        """
            返回用户的 linux 组
        """
        df = self.quota_df
        gid_quota_df = df[df.index.str.endswith(':${gid}')]
        return [row.name.replace('${gid}', str(row.quota)) for _, row in gid_quota_df.iterrows()]

    @property
    def user_linux_capabilities(self) -> List[str]:
        return self.prefix_quotas('cap:')

    def prefix_quotas(self, prefix):
        df = self.quota_df
        # 根据 quota 排序，在前面的是 default
        df2 = df[(df.index.str.startswith(prefix)) & (df.quota >= 1)].sort_values(by='quota', ascending=False)
        return [idx[len(prefix):].replace('${user_name}', self.user.user_name) for idx in df2.index.to_list()]

    @property
    def train_environments(self) -> List[str]:
        """
        返回用户能够看到的 train_environment
        @return: list
        """
        return self.prefix_quotas('train_environment:')

    @classmethod
    def df_to_jupyter_quota(cls, df: pd.DataFrame):
        res = {}
        for k, v in df.quota.to_dict().items():
            res[k.replace('jupyter:', '')] = {
                'cpu': int(str(v)[-5:-2]),
                'memory': int(str(v)[-9:-5]),
                'quota': int(str(v)[-2:])
            }
        return res

    @property
    def jupyter_quota(self):
        return self.__class__.df_to_jupyter_quota(self.quota_df[self.quota_df.index.str.startswith('jupyter:')])

    def available_priority(self, group):
        # auto 是肯定可以用的
        res_priority_dict = {
            TASK_PRIORITY.AUTO.name: TASK_PRIORITY.AUTO.value
        }
        if not self.user.is_internal:
            # 外部用户只允许使用 auto
            return res_priority_dict
        priority_mapping = {k: v for k, v in TASK_PRIORITY.items()}
        priority_mapping.pop(TASK_PRIORITY.AUTO.name)
        priority_series = self.quota_df[
            self.quota_df.index.str.startswith(f'node-{group}-') & (self.quota_df.quota > 0)
            ].index
        if len(priority_series) > 0:
            for _, _, quota_priority in priority_series.str.split('-', expand=True):
                if priority_mapping.get(quota_priority) is not None:
                    res_priority_dict[quota_priority] = priority_mapping[quota_priority]
        return res_priority_dict
