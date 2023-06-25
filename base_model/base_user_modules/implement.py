
from .default import *
from .custom import *

import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from base_model.base_task import BaseTask


class IUserNodePort(IUserModule):
    async def async_create(self, task: "BaseTask", alias: str, dist_port: int, rank: int = 0):
        raise NotImplementedError

    async def async_delete(self, task: "BaseTask", dist_port: int, rank: int = 0):
        raise NotImplementedError


class IUserQuota(IUserModule):
    async def async_set_training_quota(self, group_label, priority_label, quota, expire_time=None):
        raise NotImplementedError


class IUserAccess(IUserModule):
    async def async_create_access_token(self, from_user_name=None, access_user_name=None, access_scope=None,
                                        expire_at: datetime.datetime = datetime.datetime(3000, 1, 1)):
        raise NotImplementedError

    async def async_delete_access_token(self, access_token: str):
        raise NotImplementedError


class IUserMonitor(IUserModule):
    pass


class IUserArtifact(IUserModule):
    async def async_get_artifact(self, name, version='default', page=1, page_size=1000):
        raise NotImplementedError

    async def async_create_update_artifact(self, name, version='default', type='', location='',
                                           description='', extra='', private=False):
        raise NotImplementedError

    async def async_delete_artifact(self, name, version='default'):
        raise NotImplementedError
