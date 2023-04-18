

from __future__ import annotations

import asyncio
import base64
import weakref
import secrets
import datetime

from base_model.base_user_modules import IUserAccess
from db import MarsDB
from logm import ExceptionWithoutErrorLog
from server_model.selector import AioUserSelector
from server_model.user import User
from server_model.user_data import sync_from_db_afterwards, async_sync_from_db_afterwards
from server_model.user_data import UserAccessTokenTable


class ACCESS_SCOPE:
    ALL = 'all'  # 所有内容均可访问
    EXCEPT_JUPYTER = 'except_jupyter'  # 除了 jupyter，都可以访问

    @classmethod
    def all_scopes(cls):
        return {v for k, v in cls.__dict__.items() if k.isupper()}


class AccessTokenDescriptor(object):
    __token_dict = weakref.WeakKeyDictionary()

    def __set__(self, instance, value):
        self.__class__.__token_dict[instance] = value

    def __get__(self, instance: UserAccess, owner):
        if instance is None:
            return None
        access_token = self.__class__.__token_dict.get(instance, None)
        if access_token is not None:
            return access_token
        # 没有指定 access token，用自授权的
        df = UserAccessTokenTable.df
        user_self_access_token = df[
            (df.access_user_name == instance.user.user_name) &
            (df.from_user_name == instance.user.user_name) &
            (df.access_scope == ACCESS_SCOPE.ALL) &
            df.active
        ]
        if len(user_self_access_token) == 1:
            self.__class__.__token_dict[instance] = user_self_access_token.iloc[0].access_token
        else:
            self.__class__.__token_dict[instance] = instance.create_access_token_in_db(
                from_user_name=instance.user.user_name,
                access_user_name=instance.user.user_name,
                access_scope=ACCESS_SCOPE.ALL,
                expire_at=datetime.datetime(3000, 1, 1)
            )
        return self.__class__.__token_dict[instance]


class UserAccess(IUserAccess):
    """
    用户 access 的信息
    """
    access_token: str = AccessTokenDescriptor()

    def __init__(self, user: User):
        super().__init__(user)
        self.user = user
        self.access_scope: str = ACCESS_SCOPE.ALL
        self.from_user_name: str = self.user.user_name  # 谁拥有这个 token
        self.expire_at: datetime.datetime = datetime.datetime(3000, 1, 1)  # 过期时间

    def check_list(self):
        if self.access_scope != ACCESS_SCOPE.ALL:
            raise ExceptionWithoutErrorLog(f'您当前的准入范畴（{self.access_scope}）无法 list access tokens')

    async def async_get(self):
        self.check_list()
        df = await UserAccessTokenTable.async_df
        if not self.user.in_group('create_access_token'):
            df = df[(df.access_user_name == self.user.user_name) | (df.from_user_name == self.user.user_name)]
        return df.to_dict('records')

    async def async_create_access_token(self, from_user_name=None, access_user_name=None, access_scope=None,
                                        expire_at: datetime.datetime = datetime.datetime(3000, 1, 1)):
        """
        创建、更新 access token
        当 from_user_name access_user_name access_scope 冲突的时候，更新 expire_at
        """
        # 直接带 origin token 来访问，用作 hfai init
        if from_user_name is None and access_user_name is None and access_scope is None:
            from_user_name = self.user.user_name
            access_user_name = self.user.user_name
            access_scope = ACCESS_SCOPE.ALL
        elif from_user_name is None or access_user_name is None or access_scope is None:
            raise ExceptionWithoutErrorLog("请指定 from_user_name, access_user_name, access_scope, expire_at")
        self.check_create(access_user_name, access_scope)
        if (await AioUserSelector.find_one(user_name=from_user_name)) is None:
            raise ExceptionWithoutErrorLog(f'不存在的用户: {from_user_name}')
        if (await AioUserSelector.find_one(user_name=access_user_name)) is None:
            raise ExceptionWithoutErrorLog(f'不存在的用户: {access_user_name}')
        access_token = await self.async_create_access_token_in_db(from_user_name=from_user_name, access_user_name=access_user_name,
                                                                  access_scope=access_scope, expire_at=expire_at)
        await asyncio.sleep(1.5)    # 等待议会同步信息
        return {
            'from_user_name': from_user_name,
            'access_user_name': access_user_name,
            'access_scope': access_scope,
            'access_token': access_token,
            'expire_at': expire_at
        }

    def __insert_sql(self, from_user_name: str, access_user_name: str, access_scope: str, expire_at: datetime.datetime):
        encoded_users = base64.b16encode(f'{from_user_name}#{access_user_name}'.encode()).decode().lower()
        access_token = f'ACCESS-{encoded_users}-{secrets.token_urlsafe(24)}'
        return (
            """
            insert into "user_access_token"
            ("from_user_name", "access_user_name", "access_token", "access_scope", "expire_at", "created_by", "active")
            values (%s, %s, %s, %s, %s, %s, true)
            on conflict ("from_user_name", "access_user_name", "access_scope", "active") do update set "expire_at" = excluded."expire_at"
            returning "access_token"
            """,
            (from_user_name, access_user_name, access_token, access_scope, expire_at, self.user.user_name)
        )

    def check_create(self, access_user_name, access_scope):
        if self.user.access.access_scope != ACCESS_SCOPE.ALL:
            raise ExceptionWithoutErrorLog(f'您当前的准入范畴（{self.user.access.access_scope}）无法创建 access token')
        if not (
                access_user_name == self.user.user_name or  # 自己可以创建访问自己的 access token
                self.user.in_group('create_access_token')   # 集群管理员也可以创建
        ):
            raise ExceptionWithoutErrorLog('您无权创建 access token')
        if access_scope not in ACCESS_SCOPE.all_scopes():
            raise ExceptionWithoutErrorLog(f'无效的 access_scope: {access_scope}, 可选项: {ACCESS_SCOPE.all_scopes()}')

    @sync_from_db_afterwards(changed_tables=['user_access_token'])
    def create_access_token_in_db(self, from_user_name, access_user_name, access_scope, expire_at):
        self.check_create(access_user_name, access_scope)
        sql, params = self.__insert_sql(
            from_user_name=from_user_name,
            access_user_name=access_user_name,
            access_scope=access_scope,
            expire_at=expire_at
        )
        access_token = MarsDB(overwrite_use_db='primary').execute(sql, params).fetchall()[0][0]
        return access_token

    @async_sync_from_db_afterwards(changed_tables=['user_access_token'])
    async def async_create_access_token_in_db(self, from_user_name, access_user_name, access_scope, expire_at):
        sql, params = self.__insert_sql(
            from_user_name=from_user_name,
            access_user_name=access_user_name,
            access_scope=access_scope,
            expire_at=expire_at
        )
        access_token = (await MarsDB(overwrite_use_db='primary').a_execute(sql, params)).fetchall()[0][0]
        return access_token

    def __delete_sql(self, access_token):
        return (
            """
            update "user_access_token" set "active" = null, "deleted_by" = %s where "access_token" = %s
            """,
            (self.user.user_name, access_token)
        )

    def check_delete(self, access_token):
        if self.user.access.access_scope != ACCESS_SCOPE.ALL:
            raise ExceptionWithoutErrorLog(f'您当前的准入范畴（{self.user.access.access_scope}）无法删除 access token')
        df = UserAccessTokenTable.df
        user_access = df[df.access_token == access_token]
        if len(user_access) > 0:
            user_access = user_access.iloc[0]
            if not(
                self.user.in_group('create_access_token') or
                user_access.access_user_name == self.user.user_name
            ):
                raise ExceptionWithoutErrorLog('您无权删除这个 access token')
            if not user_access.active:
                raise ExceptionWithoutErrorLog('该 access token 已经不活跃了')
        elif not self.user.in_group('create_access_token'):
            raise ExceptionWithoutErrorLog('您无权删除这个 access token')
        else:
            raise ExceptionWithoutErrorLog(f'该 access token 不存在')

    @sync_from_db_afterwards(changed_tables=['user_access_token'])
    def delete(self, access_token):
        self.check_delete(access_token)
        sql, params = self.__delete_sql(access_token=access_token)
        MarsDB(overwrite_use_db='primary').execute(sql, params)

    @async_sync_from_db_afterwards(changed_tables=['user_access_token'])
    async def async_delete_access_token(self, access_token):
        self.check_delete(access_token)
        sql, params = self.__delete_sql(access_token=access_token)
        await MarsDB(overwrite_use_db='primary').a_execute(sql, params)
