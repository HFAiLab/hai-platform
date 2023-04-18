
from .default import *
from .custom import *

import itertools
from itertools import chain

from base_model.base_user import BaseUser
from db import MarsDB
from server_model.user_data import async_sync_from_db_afterwards


class AioUserDb(AioUserDbExtras):
    """
    用于处理和 User 表有关的数据库操作
    """
    def __init__(self, user: BaseUser):
        self.user = user

    @async_sync_from_db_afterwards(changed_tables=['user'])
    async def insert(self):
        """
        @return:
        """
        user = self.user
        sql = f'''
            insert into "user" ("user_id", "user_name", "token", "shared_group", "role", "nick_name")
            values (%s, %s, %s, %s, %s, %s)
            returning "user_id"
        '''
        params = (user.user_id, user.user_name, user.token, user.shared_group, user.role, user.nick_name)
        try:
            res = await MarsDB().a_execute(sql, params)
            user.user_id = res.fetchone().user_id
            return user
        except Exception as exp:
            print(exp)
            return None  # 运行失败

    @async_sync_from_db_afterwards(changed_tables=['quota'])
    async def insert_quota(self, resource, quota, expire_time=None, *args, **kwargs):
        """

        @param resource:
        @param quota:
        @param expire_time:
        @return:
        """
        user = self.user
        _sql = """
            insert into "quota" ("user_name", "resource", "quota", "expire_time") 
            values (%s, %s, %s, %s) 
            on conflict ("user_name", "resource") do update set "quota" = excluded."quota", "expire_time" = excluded."expire_time"
        """
        await MarsDB().a_execute(_sql, (user.user_name, resource, quota, expire_time), remote_apply=kwargs.get('remote_apply', False))

    async def insert_external_quota_change_log(self, external_user, resource, quota, original_quota, expire_time=None, *args, **kwargs):
        """

        :param external_user:
        :param resource:
        :param original_quota:
        :param quota:
        :return:
        """
        user = self.user
        _sql = """
             insert into "external_quota_change_log" ("editor", "external_user", "resource", "quota", "original_quota","expire_time") 
             values (%s, %s, %s, %s, %s, %s) 
        """
        await MarsDB().a_execute(_sql, (user.user_name, external_user, resource, quota, original_quota, expire_time), remote_apply=kwargs.get('remote_apply', False))

    @async_sync_from_db_afterwards(changed_tables=['quota'])
    async def insert_quotas(self, resource_quotas):
        """

        @param resource_quotas: [(resource, quota), (resource, quota)]
        @return:
        """

        user = self.user
        if len(resource_quotas) == 0:
            return
        params = ','.join([f"('{user.user_name}', %s, %s)"] * len(resource_quotas))
        sql = f"""
            insert into "quota" ("user_name", "resource", "quota") values {params} 
            on conflict ("user_name", "resource") do update set "quota" = excluded."quota"
        """
        await MarsDB().a_execute(sql, tuple(itertools.chain.from_iterable(resource_quotas)))

    @async_sync_from_db_afterwards(changed_tables=['quota'])
    async def delete_quota(self, resource, *args, **kwargs):
        """

        @param resource:
        @param quota:
        @return:
        """
        user = self.user
        _sql = 'delete from "quota" where "user_name" = %s and "resource" = %s'
        await MarsDB().a_execute(_sql, (user.user_name, resource), remote_apply=kwargs.get('remote_apply', False))

    async def insert_checkpoint(self, description, image_ref):
        user = self.user
        sql = 'insert into "user_image" ("user_name", "description", "image_ref") values (%s, %s, %s) on conflict do nothing'
        await MarsDB().a_execute(sql, (user.user_name, description, image_ref))

    @async_sync_from_db_afterwards(changed_tables=['user'])
    async def set_active(self, active: bool):
        user = self.user
        sql = f'''
            UPDATE "user" SET "active" = %s
            WHERE "user_id" = %s 
        '''
        await MarsDB().a_execute(sql, (active, user.user_id))

    @async_sync_from_db_afterwards(changed_tables=['user'])
    async def set_nick_name(self, nick_name: str):
        sql = f'''
            UPDATE "user" SET "nick_name" = %s
            WHERE "user_id" = %s
        '''
        await MarsDB().a_execute(sql, (nick_name, self.user.user_id))

    @async_sync_from_db_afterwards(changed_tables=['user_group'])
    async def update_groups(self, groups):
        groups = list(set(groups) - {'public', 'external', 'internal'}) # remove built-in groups
        async with MarsDB() as conn:
            await conn.execute('delete from "user_group" where "user_name" = %s', (self.user.user_name,))
            if groups:
                sql = 'insert into "user_group" ("user_name", "group") values ' + \
                      ','.join(['(%s, %s)'] * len(groups))
                params = tuple(chain.from_iterable([self.user.user_name, group] for group in groups))
                await conn.execute(sql, params)
