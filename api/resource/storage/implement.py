

from .default import *
from .custom import *

from fastapi import Depends

from api.depends import get_api_user_with_token
from server_model.user import User

async def get_user_storage_list(user: User = Depends(get_api_user_with_token())):
    """获取用户的可用挂载点路径"""
    return {
        'success': 1,
        'storages': await user.storage.async_get()
    }
