
"""
client 端对各用户组件的实现.
client 端的实现通常是对 server HTTP API 的调用，预期逻辑不会非常复杂, 暂时全部放到一个文件里,
后续如果乱了可以考虑拆分成多个文件 (参考 server_model/user_impl/user_xxx.py)
"""

from .default import *
from .custom import *

from typing import TYPE_CHECKING

from hfai.base_model.base_user_modules import IUserAccess
from hfai.base_model.base_task import BaseTask
from hfai.base_model.base_user_modules import IUserNodePort, IUserQuota, IUserStorage, IUserMonitor
from ...api.api_config import get_mars_url as mars_url
from ...api.api_utils import async_requests, RequestMethod, request_url

if TYPE_CHECKING:
    from ..user import User


class UserNodePort(IUserNodePort):
    def __init__(self, user: "User"):
        super().__init__(user)
        assert self.user.token is not None, "必须指定用户的 token"  # 出现问题时帮助 debug, 预期用户正常使用时不会 assertion failed

    def get(self):
        url = f'{mars_url()}/query/user/nodeport/list?token={self.user.token}'
        return request_url(RequestMethod.POST, url)

    async def async_get(self):
        url = f'{mars_url()}/query/user/nodeport/list?token={self.user.token}'
        return await async_requests(RequestMethod.POST, url)

    async def async_create(self, task: BaseTask, alias: str, dist_port: int, rank: int = 0):
        url = f'{mars_url()}/ugc/user/nodeport/create?token={self.user.token}&id={task.id}&usage={alias}&dist_port={dist_port}'
        result = await async_requests(RequestMethod.POST, url)
        return result

    async def async_delete(self, task: BaseTask, dist_port: int, rank: int = 0):
        task_selector = f'nb_name={task.nb_name}' if task.nb_name is not None else f'id={task.id}'
        url = f'{mars_url()}/ugc/user/nodeport/delete?token={self.user.token}&dist_port={dist_port}&' + task_selector
        return await async_requests(RequestMethod.POST, url)


class UserQuota(IUserQuota):
    async def async_get(self):
        url = f'{mars_url()}/query/user/quota/list?token={self.user.token}'
        return await async_requests(RequestMethod.POST, url)

    async def async_set_training_quota(self, group_label, priority_label, quota, expire_time=None):
        assert expire_time is None, 'client 中不支持此字段'
        url = f'{mars_url()}/operating/user/training_quota/update?token={self.user.token}&group_label={group_label}&priority_label={priority_label}&quota={quota}'
        return await async_requests(RequestMethod.POST, url)


class UserStorage(IUserStorage):
    async def async_get(self):
        url = f'{mars_url()}/monitor/user/storage/list?token={self.user.token}'
        return await async_requests(RequestMethod.POST, url)


class UserMonitor(IUserMonitor):
    async def async_get(self):
        url = f'{mars_url()}/monitor_v2/user/monitor?token={self.user.token}'
        return await async_requests(RequestMethod.GET, url)


class UserAccess(IUserAccess):
    async def async_get(self):
        url = f'{mars_url()}/query/user/access_token/list?token={self.user.token}'
        return await async_requests(RequestMethod.POST, url)   # 权限不足时返回200/success=0, 不raise
