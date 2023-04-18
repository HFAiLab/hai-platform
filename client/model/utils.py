
import os

from .user import User
from ..api.api_config import get_mars_token


__mars_user__ = None


def get_current_user() -> User:
    global __mars_user__
    if __mars_user__ is None:
        # 非集群环境只能获取到 token 字段, 其他字段获取时会请求 server 获取
        __mars_user__ = User(user_name=os.environ.get('MARSV2_USER'), user_id=os.environ.get('MARSV2_UID'),
                             token=get_mars_token(), role=os.environ.get('MARSV2_USER_ROLE'))
    return __mars_user__
