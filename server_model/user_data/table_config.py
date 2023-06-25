

import inspect
import time
from typing import Dict, Type, List

import pandas as pd

from conf.flags import TASK_PRIORITY
from db import redis_conn
from .data_table import IDataTable, DBSqlTable, ComputedTable, InMemoryTable
from .public_data_table import AutoTable, PublicDataTable, spawn_private_table
from .utils import sync_point_only

"""
    AutoSqlTable / AutoBaseTable:   所在 pod 启用议会时使用议会 cache 的数据, 否则等价于 DBSqlTable.
    DBSqlTable / DBBaseTable:       无论是否启用议会, 获取 df 时总是读取 DB 数据, 且不在议会网络中产生 diff 信息.
    ComputedTable:                  每次获取 df 时按需计算.
    InMemoryTable:                  纯议会内存表
"""


class UserTable(PublicDataTable,
    table_cls=AutoTable.AutoBaseTable,
    table_name='user',
    # 注意用户表未将 last activity 加入议会, 这一列更新太频繁, 而且只有一个时效性要求很低的 API 用到, 可以直接查从库
    columns=["user_id", "user_name", "token", "role", "active", "shared_group", "nick_name"],
):
    pass


class UserAccessTokenTable(PublicDataTable,
    table_cls=AutoTable.AutoBaseTable,
    table_name='user_access_token',
    columns=["from_user_name", "access_user_name", "access_token", "access_scope", "expire_at", "created_at", "updated_at", "created_by", "deleted_by", "active"],
):
    pass


class QuotaTable(PublicDataTable,
    table_cls=AutoTable.AutoSqlTable,
    table_name='quota',
    columns=["user_name", "resource", "quota", "expire_time"],
):
    _sql = r'''
            select
               "user_name", "resource", "quota", "expire_time", current_timestamp as "query_timestamp"
            from "quota"
            where "expire_time" is null or "expire_time" > current_timestamp
    '''

    @classmethod
    def sql(cls):
        return cls._sql


class StorageTable(PublicDataTable,
    table_cls=AutoTable.AutoBaseTable,
    table_name='storage',
    columns=["host_path", "mount_path", "owners", "conditions", "mount_type", "read_only", "action", "active"],
):
    pass


class UserAllGroupsTable(PublicDataTable,
    table_cls=AutoTable.AutoBaseTable,
    table_name='user_all_groups',
    columns=["user_name", "user_groups"],
):
    pass


class TrainEnvironmentTable(PublicDataTable,
    table_cls=AutoTable.AutoBaseTable,
    table_name='train_environment',
    columns=["env_name", "image", "schema_template", "config"],
):
    pass


class TrainImageTable(PublicDataTable,
    table_cls=AutoTable.AutoBaseTable,
    table_name='train_image',
    columns=["image_tar", "image", "path", "shared_group", "registry", "status", "task_id", "created_at", "updated_at"],
):
    pass


class AllStorageTable(PublicDataTable,
    table_cls=DBSqlTable,
    table_name='all_storage',
    columns=["host_path", "mount_path", "mount_type", "read_only", "conditions", "action", "owners", "affected_users"],
    dependencies=[UserTable, QuotaTable, StorageTable, UserAllGroupsTable],
):
    _sql = r'''
            select
               "host_path", "mount_path", "mount_type", "read_only",
               "conditions", "action",
               array_agg(distinct "single_owner") as "owners",
               array_agg(distinct array["single_owner", "user_to_group"."user_name"]::varchar[]) as "affected_users",
               current_timestamp as "query_timestamp"
            from "storage", unnest("owners") as "single_owner"
                left join (
                    select "user_name", array_cat(array["user_name"], "user_groups") as "user_groups" from "user_all_groups"
                ) as "user_to_group"
                on "single_owner" = any("user_to_group"."user_groups")
            where "storage"."active"
            group by "host_path", "mount_path", "mount_type", "read_only", "conditions", "action"
    '''
    @classmethod
    def sql(cls):
        return cls._sql


class UserMessageTable(PublicDataTable,
    table_cls=DBSqlTable,
    table_name='message',
    columns=["messageId", "important", "type", "title", "content", "detailContent", "date" , "detailText", "assigned_to"],
):
    @classmethod
    def sql(cls):
        return f'''
            select "messageId", "important", "type", "title", "content", "detailContent", "date" , "detailText", "assigned_to",
            current_timestamp as "query_timestamp"
            from "message" 
            where "expiry" > '{time.strftime('%Y-%m-%d %H:%M:%S')}'
        '''


# 简单 join 一下 User 表和 UserAllGroups 表, 方便使用
class UserWithAllGroupsTable(PublicDataTable,
    table_cls=ComputedTable,
    table_name='user_with_all_groups',
    columns=["user_id", "user_name", "token", "role", "active", "shared_group", "nick_name", "user_groups"],
    dependencies=[UserTable, UserAllGroupsTable],
):
    @classmethod
    def compute(cls):
        user_df, user_group_df = UserTable.get_df(), UserAllGroupsTable.get_df()
        return pd.merge(user_df, user_group_df, how='left', on='user_name')

    @classmethod
    @sync_point_only
    def update_hook(cls):
        redis_conn.set('all_user_info_last_update_time', time.time_ns())


class UserAllQuotaTable(PublicDataTable,
    table_cls=ComputedTable,
    table_name='user_all_quota',
    columns=["user_name", "hit_group", "resource", "quota", "expire_time"],
    dependencies=[QuotaTable, UserAllGroupsTable],
):
    @classmethod
    def compute(cls):
        quota_df, user_group_df = QuotaTable.get_df(), UserAllGroupsTable.get_df()
        user_group_df.user_groups += user_group_df.user_name.apply(lambda x: [x])
        merged_df = user_group_df.explode('user_groups') \
            .merge(quota_df, how='inner', left_on='user_groups', right_on='user_name', suffixes=('', '_quota'))
        return merged_df.rename({'user_groups': 'hit_group'}, axis='columns').drop(['user_name_quota'], axis='columns')

    @classmethod
    @sync_point_only
    def update_hook(cls):
        redis_conn.set('all_user_quota_last_update_time', time.time_ns())


class SchedulerUserTable(PublicDataTable,
    table_cls=ComputedTable,
    table_name='scheduler_user',
    columns=["user_name", "hit_group", "resource", "group", "quota", "role", "priority", "expire_time", "active"],
    dependencies=[UserAllQuotaTable, UserTable],
):
    @classmethod
    def interpret_node_resource(cls, resource):
        # resource 列的处理, 其值为 [node-{group}-{priority}] or [node_limit-{group}-{priority}]
        # or [jupyter:xxx] or [background_task]
        # 这里可以能有node_limit[weka]这种情况，在scheduler这里统一成node_limit，同样条件取最小值即可
        if resource.startswith('node-') or resource.startswith('node_limit') or resource.startswith('node_role_limit'):
            resource, group, priority = resource.split('-')
            if '[' in resource:
                resource = resource.split('[')[0]
            priority = TASK_PRIORITY[priority].value
            return resource, priority, group
        else:
            return resource, 0, None

    @classmethod
    def compute(cls):
        user_df, quota_df = UserTable.get_df(), UserAllQuotaTable.get_df()
        node_quota_mask = quota_df.resource.str.fullmatch('node.*-.+-.+') | quota_df.resource.str.startswith('jupyter:')
        quota_df = quota_df[node_quota_mask].reset_index(drop=True)
        # 提取 resource 列的信息
        resource_df = pd.DataFrame(quota_df.resource.apply(cls.interpret_node_resource).tolist())
        quota_df['resource'], quota_df['priority'], quota_df['group'] = [resource_df[col] for col in resource_df]
        # 处理优先级, 用户 quota 优先于 组 quota， 内部quota limit取最小，quota取最大
        quota_df['is_user_quota'] = quota_df.user_name == quota_df.hit_group
        # 先把自己的选出来
        user_quota_df_limit = quota_df[quota_df.is_user_quota & quota_df.resource.str.startswith('node_limit')]\
            .sort_values('quota', ascending=True).groupby(['user_name', 'resource', 'group', 'priority'], dropna=False, sort=False).nth(0)
        user_quota_df = quota_df[quota_df.is_user_quota & ~quota_df.resource.str.startswith('node_limit')]\
            .sort_values('quota', ascending=False).groupby(['user_name', 'resource', 'group', 'priority'], dropna=False, sort=False).nth(0)
        user_quota_df = pd.concat([user_quota_df, user_quota_df_limit])
        # 再选没有个人的
        group_quota_df_limit = quota_df[~quota_df.is_user_quota & quota_df.resource.str.startswith('node_limit')]\
            .sort_values('quota', ascending=True).groupby(['user_name', 'resource', 'group', 'priority'], dropna=False, sort=False).nth(0)
        group_quota_df = quota_df[~quota_df.is_user_quota & ~quota_df.resource.str.startswith('node_limit')]\
            .sort_values('quota', ascending=False).groupby(['user_name', 'resource', 'group', 'priority'], dropna=False, sort=False).nth(0)
        group_quota_df = pd.concat([group_quota_df, group_quota_df_limit])
        quota_df = pd.concat([user_quota_df, group_quota_df]).reset_index().drop(['is_user_quota'], axis='columns')
        quota_df = quota_df.drop_duplicates(subset=['user_name', 'resource', 'group', 'priority'], keep='first')
        quota_df.group = quota_df.group.apply(lambda g: None if g != g else g)
        return pd.merge(user_df[['user_name', 'role', 'active']], quota_df, how='inner')


all_tables : List[Type[PublicDataTable]] = \
    [x for x in globals().values() if inspect.isclass(x) and issubclass(x, PublicDataTable) and x != PublicDataTable]
TABLES: Dict[str, Type[IDataTable]] = {}
__all__ = [table_cls.__name__ for table_cls in all_tables]


# Config sanity check
assert all(table.init_kwargs.get('table_name') is not None and table.init_kwargs.get('columns') is not None for table in all_tables), \
    "TableConfig 配置有误, 必须指定 table_name 和 columns"
assert len(set(tb.table_name for tb in all_tables)) == len(all_tables), "定义的 table_name 有重复"


def spawn_private_tables(is_roaming_enabled):
    for public_cls in all_tables:
        spawn_private_table(public_cls, enable_parliament=is_roaming_enabled)
        TABLES[public_cls.table_name] = public_cls.private_table
