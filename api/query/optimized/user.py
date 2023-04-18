

import pandas as pd
from fastapi import Depends

from api.depends import get_api_user_with_token
from api.user.priority_utils import check_permission
from server_model.user import User
from server_model.user_data import UserWithAllGroupsTable, SchedulerUserTable, UserAllGroupsTable, UserAllQuotaTable
from conf.flags import USER_ROLE


async def get_user_api(user: User = Depends(get_api_user_with_token())):
    return {
        'success': 1,
        'result': await user.async_get_info()
    }


async def get_all_user_api(user: User = Depends(get_api_user_with_token())):
    if not user.is_internal:
        return {
            'success': 0,
            'msg': '无权使用该接口'
        }
    user_df = await UserWithAllGroupsTable.async_df
    users = user_df.to_dict('records')
    for user in users:
        user.pop('token')
    return {
        'success': 1,
        'result': users
    }


async def get_user_node_quota_api(role: str , user: User = Depends(get_api_user_with_token())):
    """ 获取配置到用户账号名下的节点 quota 和 node limit, 不包括从 group/shared_group 中继承的数据 """
    if role == USER_ROLE.EXTERNAL:
        check_permission(user, 'external_quota_editor')
    elif role == USER_ROLE.INTERNAL:
        check_permission(user, 'internal_quota_limit_editor')
    else:
        return {
            'success': 0,
            'msg': f'不存在的 role: {role}'
        }
    quota_df = await SchedulerUserTable.async_df
    quota_df = quota_df[(quota_df.user_name == quota_df.hit_group) & (quota_df.role == role)]
    json_agg = lambda k, v: lambda df: pd.DataFrame([[df[[k, v]].set_index(k).to_dict()[v]]], columns=[v])
    quota_df = quota_df \
        .groupby(['user_name', 'role', 'group', 'resource']) \
            .apply(json_agg(k='priority', v='quota')).reset_index(-1, drop=True).reset_index() \
        .groupby(['user_name', 'role', 'group']) \
            .apply(json_agg(k='resource', v='quota')).reset_index(-1, drop=True).reset_index() \
        .groupby(['user_name', 'role']) \
            .apply(json_agg(k='group', v='quota')).reset_index(-1, drop=True).reset_index()
    user_group_df = await UserAllGroupsTable.async_df
    quota_df = quota_df.merge(user_group_df, how='left', on='user_name')

    return {
        'success': 1,
        'result': quota_df.to_dict('records')
    }


async def get_all_user_node_quota_api(user: User=Depends(get_api_user_with_token(allowed_groups=['internal']))):
    """ 获取全部用户的所有节点 quota 和 node limit """
    def process_df(df, prefix):
        df = df[df.resource.str.startswith(prefix)].copy()
        df['resource'] = df.resource.str.slice(len(prefix))
        return df.groupby('user_name').apply(lambda x: x.set_index('resource').to_dict()['quota']).to_dict()

    quota_df = await UserAllQuotaTable.async_df
    quota_df = quota_df[['user_name', 'resource', 'quota']]
    node_quota, node_limit = process_df(quota_df, prefix='node-'), process_df(quota_df, prefix='node_limit')
    node_limit_extra = {}
    for user_name in list(node_limit.keys()):
        node_limit_extra[user_name] = {}
        for key in list(node_limit[user_name].keys()):
            resource, group, priority = key.split('-')
            replaced_key = f'{group}-{priority}'
            node_limit[user_name][replaced_key] = node_limit[user_name][key]\
                if replaced_key not in node_limit[user_name] else\
                min(node_limit[user_name][key], node_limit[user_name][replaced_key])
            node_limit_extra[user_name][f'{replaced_key}{resource.strip("-")}'] = node_limit[user_name].pop(key)
    res = {
        user_name: {'node': node_quota.get(user_name, {}), 'node_limit': node_limit.get(user_name, {}), 'node_limit_extra': node_limit_extra.get(user_name, {})}
        for user_name in quota_df.user_name.tolist()
    }
    return {'success': 1, 'result': res}


async def get_quota_used_api(user: User = Depends(get_api_user_with_token())):
    worker_user_info = {}
    worker_user_info['user_name'] = user.user_name
    worker_user_info['quota'] = user.quota.node_quota
    worker_user_info['quota_limit'] = user.quota.node_quota_limit
    worker_user_info['quota_limit_extra'] = user.quota.node_quota_limit_extra
    worker_user_info['all_quota'] = {k.replace('node-', ''): v for k, v in user.quota.node_quota.items()}
    worker_user_info['already_used_quota'] = await user.quota.async_get_used_quota()
    return {
        'success': 1,
        'result': worker_user_info
    }
