

import pandas as pd
from fastapi import Depends, HTTPException

from api.depends import get_api_user_with_token
from logm import logger
from server_model.selector import AioUserSelector
from server_model.user import User


async def set_user_gpu_quota(token, group_label: str, priority_label: str, quota: int):
    # 获取最后一次调度时，用户的权利数等信息
    user = await AioUserSelector.find_one(token=token)
    if not user.is_internal:
        raise HTTPException(403, detail=f'设置 quota 失败: 非内部用户不允许使用这个接口')
    try:
        await user.quota.async_set_training_quota(group_label, priority_label, quota)
        return {
            'success': 1,
            'quota': {
                f'node-{group_label}-{priority_label}': quota
            }
        }
    except Exception as e:
        logger.exception(e)
        raise HTTPException(403, detail=f'设置 quota 失败: {str(e)}') from e


async def get_user_all_quota(user: User = Depends(get_api_user_with_token())):
    return {
        'success': 1,
        'result': await user.quota.async_get()
    }
