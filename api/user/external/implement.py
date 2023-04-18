
from .default import *
from .custom import *


from datetime import datetime
import pandas as pd
from fastapi import Depends, HTTPException

from api.depends import get_api_user_with_token
from conf.flags import USER_ROLE, TASK_PRIORITY
from server_model.selector import AioUserSelector
from server_model.user import User
from server_model.user_data import SchedulerUserTable, UserAllGroupsTable
from api.user.priority_utils import get_priority_str


async def get_external_user_priority_quota(user: User = Depends(get_api_user_with_token(allowed_groups=[EXTERNAL_QUOTA_EDITOR]))):
    """
    用于获取外部用户的 quota 列表，和获取内部用的列表，两个接口
    :return:
    """
    user_df = await SchedulerUserTable.async_df
    user_group_df = await UserAllGroupsTable.async_df
    user_df = pd.merge(user_df, user_group_df, on='user_name')
    user_df = user_df[user_df.resource.str.startswith('node') & (user_df.role == USER_ROLE.EXTERNAL)
                      & (user_df.group != '') & user_df.group]
    return {
        'success': 1,
        'data': user_df.to_dict('records')
    }


async def set_external_user_quota(
        external_username: str,
        priority: int,
        group: str,
        quota: int,
        expire_time: str = None,
        user: User = Depends(get_api_user_with_token(allowed_groups=[EXTERNAL_QUOTA_EDITOR])),
):
    if priority > TASK_PRIORITY.NORMAL.value and expire_time is None:
        raise HTTPException(status_code=403, detail='设置高于 NORMAL 优先级的 quota 必须指定 expire_time')
    if expire_time is not None:
        try:
            expire_time = datetime.fromisoformat(expire_time)
        except Exception as e:
            raise HTTPException(status_code=403, detail=f'expire_time 格式不对： {e}。正确格式示例：2022-01-01 00:00:00')

    priority_str = get_priority_str(priority)
    external_user = await AioUserSelector.find_one(user_name=external_username)

    if external_user is None:
        return HTTPException(status_code=404, detail=f'用户 {external_username} 不存在!')

    if external_user.role != USER_ROLE.EXTERNAL:
        raise HTTPException(status_code=403, detail='只能修改外部用户的quota')

    await external_user.quota.async_set_training_quota(group_label=group, priority_label=priority_str, quota=quota, expire_time=expire_time)

    return {
        'success': 1,
        'msg': '修改成功'
    }
