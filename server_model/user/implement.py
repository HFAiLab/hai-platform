
from __future__ import annotations

from .default import *
from .custom import *

import pandas as pd
from cached_property import cached_property
from typing import TYPE_CHECKING, List

from db import MarsDB
from base_model.base_user import BaseUser
from conf.flags import USER_ROLE
from server_model.user_data import UserWithAllGroupsTable

if TYPE_CHECKING:
    from server_model.user_impl import *


class TokenDescriptor(object):
    def __set__(self, instance, value):
        if value.startswith('ACCESS-'):
            instance.access.access_token = value

    def __get__(self, instance, instance_type=None):
        if instance is None:    # 访问类属性时 instance 参数为 None, 如 `User.token`, 特判防止出问题
            return None
        return instance.access.access_token


class User(UserExtras, BaseUser):
    token = TokenDescriptor()

    def __init__(self, user_name, user_id, token, role, active, **kwargs):
        super().__init__(user_name, user_id, token, role, active, **kwargs)
        self._group_list = (kwargs['user_groups'] + [user_name]) if 'user_groups' in kwargs else None
        # 获取一下，初始化 token
        self.token = self.token

    def __repr__(self):
        self_dict = self.__dict__
        return '\n'.join([f'{k}: {self_dict[k]}' for k in self_dict])

    # 前后端通用的用户实例属性
    quota: UserQuota = ServerUserModule()
    nodeport: UserNodePort = ServerUserModule()
    storage: UserStorage = ServerUserModule()
    access: UserAccess = ServerUserModule()
    monitor: UserMonitor = ServerUserModule()
    image: UserImage = ServerUserModule()

    # 仅后端使用的业务组件
    config: UserConfig = ServerUserModule()
    db: UserDb = ServerUserModule()
    aio_db: AioUserDb = ServerUserModule()
    checkpoint: UserCheckpoint = ServerUserModule()
    environment: UserEnvironment = ServerUserModule()
    message: UserMessage = ServerUserModule()

    def get_info(self):
        return {
            'user_name': self.user_name,
            'group_list': self.group_list,
            'shared_group': self.shared_group,
            'token': self.token,
            'user_shared_group': self.shared_group,
            'user_group': self.group_list,
            'user_id': self.user_id,
            'role': self.role,
            'nick_name': self.nick_name,
            'access_scope': self.access.access_scope
        }

    @property
    def group_list(self):
        if self._group_list is None:
            df = UserWithAllGroupsTable.df
            self._group_list = df[df.user_id == self.user_id].iloc[0].user_groups + [self.user_name]
        return self._group_list

    def in_all_groups(self, groups: List[str]):
        return self.active and ('root' in self.group_list or len(set(self.group_list) & set(groups)) == len(groups))

    def in_any_group(self, groups: List[str]):
        return self.active and ('root' in self.group_list or any(set(self.group_list) & set(groups)))

    def in_group(self, group: str):
        return self.in_all_groups([group])

    @property
    def is_internal(self):
        return self.role == USER_ROLE.INTERNAL

    @property
    def uid(self):
        return self.user_id

    @cached_property
    def db_str_group_list(self) -> str:
        """
        返回用户在数据库用来 select WHERE user_name in ({db_str_group_list})
        @return:
        """
        return ','.join(map(lambda n: f"'{n}'", self.group_list))

    @cached_property
    def other_shared_group_users(self) -> pd.DataFrame:
        return pd.read_sql(f"""
        select "user_name", "user_id" from "user"
        where "user_name" != '{self.user_name}' and "shared_group" = '{self.shared_group}'
        """, MarsDB().db)
