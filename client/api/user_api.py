from .api_config import get_mars_url as mars_url
from .api_config import get_mars_token as mars_token
from .api_utils import async_requests, RequestMethod
from ..model.user import User


async def get_user_info(*args, **kwargs):
    """
    获取用户信息
    :param kwargs['token']: 用户token
    :return: user_info_dict
    """
    user = User(token=kwargs.get('token', mars_token()))
    return await user.async_get_info()


async def get_worker_user_info(**kwargs):
    """
    获取worker里记录的用户信息
    """
    token = kwargs.get('token', mars_token())
    url = f'{mars_url()}/query/user/training_quota/get_used?token={token}'
    result = await async_requests(RequestMethod.POST, url)
    return result['result']


async def set_user_gpu_quota(group_label: str, priority_label: str, quota: int, **kwargs):
    """
    更改用户的显卡配额
    """
    user = User(token=kwargs.get('token', mars_token()))
    result = await user.quota.async_set_training_quota(group_label, priority_label, quota)
    return result['quota']
