
from .default import *
from .custom import *

import os
from typing import List

import pandas as pd
from fastapi import Depends, HTTPException, Query
from pydantic import BaseModel

from api.depends import get_api_user_with_token
from api.user.priority_utils import get_priority_str
from base_model.base_user import BaseUser
from conf import CONF
from conf.flags import USER_ROLE, TASK_PRIORITY
from db import MarsDB
from logm import logger
from server_model.user_data import SchedulerUserTable, UserAllGroupsTable
from server_model.selector import AioUserSelector
from server_model.user import User
from server_model.user_impl import AioUserDb


async def get_internal_user_priority_quota(user: User = Depends(get_api_user_with_token(allowed_groups=['internal_quota_limit_editor']))):
    """
    用于获取外部用户的 quota 列表，和获取内部用的列表，两个接口
    :return:
    """
    user_df = await SchedulerUserTable.async_df
    user_group_df = await UserAllGroupsTable.async_df
    user_df = pd.merge(user_df, user_group_df, on='user_name')
    user_df = user_df[user_df.resource.str.startswith('node') & (user_df.role == USER_ROLE.INTERNAL)
                      & (user_df.group != '') & user_df.group]
    return {
        'success': 1,
        'data': user_df.to_dict('records')
    }


async def set_user_gpu_quota_limit(
        internal_username: str,
        priority: int,
        group: str,
        quota: int,
        user: User = Depends(get_api_user_with_token(allowed_groups=['internal_quota_limit_editor'])),
):
    if priority < TASK_PRIORITY.BELOW_NORMAL.value:
        raise HTTPException(status_code=403, detail={
            'success': 0,
            'msg': '不能设置更低优先级'
        })

    priority_str = get_priority_str(priority)
    internal_user = await AioUserSelector.find_one(user_name=internal_username)
    if internal_user.role != USER_ROLE.INTERNAL:
        raise HTTPException(status_code=403, detail={
            'success': 0,
            'msg': '只能修改内部用户的 quota_limit'
        })

    resource = f'node_limit-{group}-{priority_str}'

    # 修改 quota_limit
    await internal_user.aio_db.insert_quota(resource, quota, remote_apply=False)

    return {
        'success': 1,
        'msg': '修改成功'
    }


async def set_user_active_state_api(user_name: str, active: bool,
                                    user: User = Depends(get_api_user_with_token(allowed_groups=['developer_admin']))):
    user = await AioUserSelector.find_one(user_name=user_name)
    if user is None:
        raise HTTPException(404, detail='用户不存在')
    await user.aio_db.set_active(active)
    return {
        'success': 1,
        'msg': '设置成功'
    }


async def update_user_group(user_name: str, groups: List[str] = Query(default=[]),
                            api_user: User = Depends(get_api_user_with_token(allowed_groups=['root']))):
    user = await AioUserSelector.find_one(user_name=user_name)
    if user is None:
        raise HTTPException(404, detail='用户不存在')
    await user.aio_db.update_groups(groups)
    return {
        'success': 1, 'msg': '设置成功'
    }


class RestUser(BaseModel):
    user_name: str
    shared_group: str
    user_id: int = None
    role: str = USER_ROLE.INTERNAL
    nick_name: str = None
    active: bool = True


async def create_user_api(user: RestUser, api_user=Depends(
            get_api_user_with_token(allowed_groups=['cluster_manager', 'ops', 'developer_admin']))):
    dup = (await MarsDB().a_execute('select * from "user" where "user_id" = %s or "user_name" = %s',
                                    (user.user_id, user.user_name))).fetchone()
    if dup is not None and dup.user_name == user.user_name:
        return {'success': 0, 'msg': f'user_name [{user.user_name}] 已存在'}
    if dup is not None and dup.user_id == user.user_id:
        return {'success': 0, 'msg': f'user_id [{user.user_id}] 已存在'}

    user.nick_name = user.nick_name or user.user_name
    user = BaseUser(**user.dict(), token=None)  # 这里如果用 server 端 User 类会自动创建 access token, 而且拿不到原始 token
    if (user := await AioUserDb(user).insert()) is None:
        return {'success': 0, 'msg': '创建用户失败'}

    # 初始化必要的目录
    try:
        log_dir = {d.role : d.dir for d in CONF.experiment.log.dist}.get(user.role).replace('{user_name}', user.user_name)
        jupyter_dir = CONF.jupyter.builtin_services.jupyter.environ[user.role].JUPYTER_DIR.replace('{user_name}', user.user_name)
        for path, mode in zip([log_dir, jupyter_dir], [0o755, 0o700]):
            os.makedirs(path, exist_ok=True)
            os.chmod(path, mode=mode)
            os.chown(path, uid=user.user_id, gid=user.user_id)
    except Exception as e:
        logger.exception(e)
        return {'success': 0, 'msg': '创建用户目录失败, 请联系管理员'}

    # 执行额外的脚本
    if (script_path := CONF.get('server', {}).get('create_user_extra_script_path')) is not None:
        set_env = ' '.join(f'{k}={v}' for k, v in {
            'USER_ID': user.user_id, 'USER_NAME': user.user_name, 'USER_SHARED_GROUP': user.shared_group, 'USER_ROLE': user.role
        })
        if os.system(f'{set_env} /bin/bash {script_path}') != 0:
            return {'success': 0, 'msg': '用户创建完成, 但 extra script 执行失败'}

    return {
        'success': 1,
        'msg': '创建用户成功',
        'result': {
            'user_id': user.user_id,
            'user_name': user.user_name,
            'token': user.token
        }
    }
