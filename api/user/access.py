

import datetime
from fastapi import Depends

from server_model.user import User
from api.depends import get_api_user_with_token
from logm import logger
from server_model.user_impl.user_access import ACCESS_SCOPE


async def create_access_token(
        from_user_name: str = None,  # 这个 token 给谁用的
        access_user_name: str = None,  # 代表了谁的身份
        access_scope: str = None,  # 准入范畴
        expire_at: datetime.datetime = datetime.datetime(3000, 1, 1),  # 过期时间
        user: User = Depends(get_api_user_with_token())
):
    try:
        result = await user.access.async_create_access_token(from_user_name, access_user_name, access_scope, expire_at)
        return {
            'success': 1,
            'result': result
        }
    except Exception as e:
        logger.exception(e)
        return {
            'success': 0,
            'msg': str(e)
        }


async def delete_access_token(access_token: str, user: User = Depends(get_api_user_with_token())):
    try:
        await user.access.async_delete_access_token(access_token=access_token)
        return {
            'success': 1,
            'msg': '删除成功'
        }
    except Exception as e:
        logger.exception(e)
        return {
            'success': 0,
            'msg': str(e)
        }


async def list_access_token(user: User = Depends(get_api_user_with_token(allowed_scopes=[ACCESS_SCOPE.ALL]))):
    try:
        return {
            'success': 1,
            'result': {
                'access_tokens': await user.access.async_get()
            }
        }
    except Exception as e:
        logger.exception(e)
        return {
            'success': 0,
            'msg': str(e)
        }
