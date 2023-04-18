
from .default import *
from .custom import *

from hfai.base_model.base_user import BaseUser, UserModuleDescriptor
from hfai.conf.flags import USER_ROLE
from ...api.api_config import get_mars_url as mars_url
from ...api.api_utils import async_requests, RequestMethod, request_url
from ..user_impl import UserNodePort, UserQuota, UserStorage, UserAccess, UserMonitor, UserImage


class RemoteInfoDescriptor:
    """ 本地拿不到的用户属性, 尝试获取时先请求后端 get_info 接口 """
    def __set_name__(self, owner, name):
        self.private_name = '_' + name
    def __set__(self, instance: "User", value):
        setattr(instance, self.private_name, value)

    def __get__(self, instance: "User", type=None):
        if instance is None:
            return self
        if not hasattr(instance, self.private_name) or getattr(instance, self.private_name) is None:
            instance.get_info()
        return getattr(instance, self.private_name)


class User(UserExtras, BaseUser):
    nodeport: UserNodePort = UserModuleDescriptor()
    quota: UserQuota = UserModuleDescriptor()
    storage: UserStorage = UserModuleDescriptor()
    access: UserAccess = UserModuleDescriptor()
    monitor: UserMonitor = UserModuleDescriptor()
    image: UserImage = UserModuleDescriptor()

    user_id = RemoteInfoDescriptor()
    user_name = RemoteInfoDescriptor()
    nick_name = RemoteInfoDescriptor()
    role = RemoteInfoDescriptor()
    shared_group = RemoteInfoDescriptor()
    group_list = RemoteInfoDescriptor()
    access_scope = RemoteInfoDescriptor()
    active = RemoteInfoDescriptor()

    def __init__(self, token, user_name=None, user_id=None, role=None, active=None, **kwargs):
        self._remote_info = None
        super(User, self).__init__(token=token, user_name=user_name, user_id=user_id, role=role, active=active, **kwargs)

    @property
    def is_internal(self):
        return self.role == USER_ROLE.INTERNAL

    def get_info(self):
        if self._remote_info is None:
            info = request_url(RequestMethod.POST, f'{mars_url()}/query/user/info?token={self.token}')
            self._remote_info = info['result']
            self._update_remote_info()
        return self._remote_info

    async def async_get_info(self):
        if self._remote_info is None:
            info = await async_requests(RequestMethod.POST, f'{mars_url()}/query/user/info?token={self.token}')
            self._remote_info = info['result']
            self._update_remote_info()
        return self._remote_info

    def _update_remote_info(self):
        for k, v in self._remote_info.items():
            if hasattr(User, k) and isinstance(getattr(User, k), RemoteInfoDescriptor):
                setattr(self, k, v)
