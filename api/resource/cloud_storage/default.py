
from conf import CONF

async def get_sts_token():
    return {'success': 1, f'{CONF.cloud.storage.provider}': {}, 'msg': 'not implemented'}


async def set_external_user_cloud_storage_quota():
    return {'success': 1, 'msg': 'not implemented'}


async def set_sync_status():
    return {'success': 1, 'msg': 'not implemented'}


async def get_sync_status():
    return {'success': 1, 'data': [], 'msg': 'not implemented'}


async def delete_files():
    return {'success': 1, 'msg': 'not implemented'}


async def list_cluster_files():
    return []


async def sync_to_cluster():
    return {'success': 1, 'msg': 'not implemented', 'index': None, 'dst_path': None}


async def sync_to_cluster_status():
    return {'success': 1, 'status': None, 'msg': 'not implemented'}


async def sync_from_cluster():
    return {'success': 1, 'msg': 'not implemented', 'index': None}


async def sync_from_cluster_status():
    return {'success': 1, 'status': None, 'msg': 'not implemented'}
