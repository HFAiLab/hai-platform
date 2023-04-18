from .api_config import get_mars_token as mars_token
from ..model import User


async def get_user_personal_storage(**kwargs):
    """
    获取用户的存储路径
    """
    user = User(token=kwargs.get('token', mars_token()))
    result = await user.storage.async_get()
    return result['storages']


async def get_user_group(**kwargs):
    """
    获取用户的group信息
    @param kwargs:
    @return:
    """
    user = User(token=kwargs.get('token', mars_token()))
    result = await user.async_get_info()
    return result
