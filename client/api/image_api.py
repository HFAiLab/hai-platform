
from .api_config import get_mars_token as mars_token
from ..model import User


async def fetch_images(**kwargs):
    """
    :param kwargs:
    :return: mars_images, user_images
    """
    user = User(token=kwargs.get('token', mars_token()))
    result = await user.image.async_get()
    return result.get('result', {}).get('mars_images', []), result.get('result', {}).get('user_images', [])


async def load_image_tar(tar, **kwargs):
    """
    :param tar: 镜像 tar 包的路径
    :param kwargs:
    :return:
    """
    user = User(token=kwargs.get('token', mars_token()))
    result = await user.image.async_load(tar)
    print(result['msg'])


async def delete_image_by_name(image_name, **kwargs):
    """
    :param image_name: 镜像的名字
    :param kwargs:
    :return:
    """
    user = User(token=kwargs.get('token', mars_token()))
    result = await user.image.async_delete(image_name)
    print(result['msg'])
