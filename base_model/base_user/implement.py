
from .default import *
from .custom import *

import secrets
import string
import random

from ..base_user_modules import *

class BaseUser(BaseUserExtras):
    nodeport: IUserNodePort = UserModuleDescriptor()
    quota: IUserQuota = UserModuleDescriptor()
    storage: IUserStorage = UserModuleDescriptor()
    access: IUserAccess = UserModuleDescriptor()
    monitor: IUserMonitor = UserModuleDescriptor()
    artifact: IUserArtifact = UserModuleDescriptor()

    # 防止实现类忘记实现子组件中的接口, 在 import 时就抛出异常
    def __init_subclass__(cls, **kwargs):
        for attr in dir(cls):
            module = getattr(cls, attr)
            if isinstance(module, UserModuleDescriptor) and hasattr(BaseUser, attr):
                assert id(module) != id(getattr(BaseUser, attr)), f'{cls} 类的 Module [{attr}] 仍然是接口类, 未被实现!'

    def __init__(self, user_name, user_id, token, role, active, **kwargs):
        self.user_name = user_name
        self.user_id = user_id
        if token is None:
            token = ''.join(random.choices(string.ascii_letters, k=4)) + secrets.token_urlsafe(12)
        self.token = token
        self.role = role
        self.active = active
        self.shared_group = kwargs.get('shared_group')
        self.nick_name = kwargs.get('nick_name', user_name)

    def __repr__(self):
        self_dict = self.__dict__
        return '\n'.join([f'{k}: {self_dict[k]}' for k in self_dict])

    def get_info(self):
        raise NotImplementedError

    async def async_get_info(self):
        return self.get_info()
