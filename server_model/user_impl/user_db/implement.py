
from .default import *
from .custom import *

from base_model.base_user import BaseUser
from db import MarsDB
from server_model.user_data import sync_from_db_afterwards


class UserDb(UserDbExtras):
    """
    用于处理和 User 表有关的数据库操作
    """
    def __init__(self, user: BaseUser):
        self.user = user

    @sync_from_db_afterwards(changed_tables=['user'])
    def insert(self):
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
            res = MarsDB().execute(sql, params)
            user.user_id = res.fetchone().user_id
            return user
        except Exception as exp:
            print(exp)
            return None  # 运行失败

    @sync_from_db_afterwards(changed_tables=['quota'])
    def insert_quota(self, resource, quota):
        """

        @param resource:
        @param quota:
        @return:
        """
        user = self.user
        sql = """
            insert into "quota" ("user_name", "resource", "quota") 
            values (%s, %s, %s) 
            on conflict ("user_name", "resource") do update set "quota" = excluded."quota"
        """
        MarsDB().execute(sql, (user.user_name, resource, quota))

    @sync_from_db_afterwards(changed_tables=['quota'])
    def delete_quota(self, resource):
        """

        @param resource:
        @param quota:
        @return:
        """
        user = self.user
        sql = 'delete from "quota" where "user_name" = %s and "resource" = %s'
        MarsDB().execute(sql, (user.user_name, resource))

    def insert_checkpoint(self, description, image_ref):
        user = self.user
        sql = 'insert into "user_image" ("user_name", "description", "image_ref") values (%s, %s, %s) on conflict do nothing'
        MarsDB().execute(sql, (user.user_name, description, image_ref))

    @sync_from_db_afterwards(changed_tables=['user'])
    def set_active(self, active: bool):
        user = self.user
        _sql = f'''
            UPDATE "user" SET "active" = %s
            WHERE "user_id" = %s 
        '''
        MarsDB().execute(_sql, (active, user.user_id))
