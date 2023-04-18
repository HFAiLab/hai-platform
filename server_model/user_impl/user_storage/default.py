
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .implement import UserStorage


class UserStorageExtras:
    async def async_get(self: UserStorage):
        storages = self.personal_storage()
        for s in storages:
            s['quota'] = {}
            if not self.user.is_internal:
                del s['host_path']
            del s['name']
        return storages

    async def async_get_usage(self: UserStorage, storage_usage=None):
        # 可使用 `monitor.async_get_storage_usage` 编写查询用户存储用量的逻辑
        return {}
