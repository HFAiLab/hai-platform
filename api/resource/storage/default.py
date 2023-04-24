
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .implement import MountPoint


async def update_cluster_venv():
    return {'success': 1, 'msg': 'not implemented', 'path': None}


async def get_user_weka_usage():
    return {'success': 1, 'result': {}, 'msg': 'not implemented'}


async def get_3fs_monitor_dir_api():
    return []


async def get_external_user_storage_usage():
    return {'success': 1, 'result': {}, 'msg': 'not implemented'}


async def security_check(mount_point: MountPoint):
    """ 添加挂载点时的安全校验 """
    pass
