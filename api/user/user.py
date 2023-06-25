
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
    return {'success': 1, 'result': await user.quota.async_get()}


async def get_user_artifact(name,
                            version='default',
                            page: int = 1,
                            page_size: int = 1000,
                            user: User = Depends(get_api_user_with_token())):
    if not name:
        return {'success': 0, "msg": 'artifact name not set'}
    return {
        'success': 1,
        'msg': await user.artifact.async_get(name, version, page, page_size)
    }


async def create_update_user_artifact(name,
                                      version='default',
                                      type='',
                                      location='',
                                      description='',
                                      extra='',
                                      private: bool =False,
                                      user: User = Depends(get_api_user_with_token())):
    if not name:
        return {'success': 0, "msg": 'artifact name not set'}
    try:
        await user.artifact.async_create_update_artifact(name, version, type,
                                                         location, description,
                                                         extra, private)
        return {
            'success': 1,
            'msg': f'create or update artifact {name}:{version} success'
        }
    except Exception as e:
        return {
            'success': 0,
            'msg': f'create or update artifact {name}:{version} failed, {str(e)}'
        }


async def delete_user_artifact(name,
                               version='default',
                               user: User = Depends(
                                   get_api_user_with_token())):
    if not name:
        return {'success': 0, "msg": 'artifact name not set'}
    count = await user.artifact.async_delete_artifact(name, version)
    if count == 0:
        return {
            'success': 1,
            'msg': f'{name}:{version}可能已经被删除，或者您不是owner'
        }
    return {
        'success': 1,
        'msg': f'delete artifact {name}:{version}, count: {count}'
    }
