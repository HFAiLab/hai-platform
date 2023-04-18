import os
import sys
from .api_config import get_mars_url as mars_url
from .api_config import get_mars_token as mars_token
from .api_utils import async_requests, RequestMethod
from hfai.conf.utils import FileType
from haienv.client.model import Haienv

# push上来的暂不支持上传extra_path
async def push_venv(venv_name, force, no_checksum, no_zip, no_diff, list_timeout, sync_timeout, cloud_connect_timeout, token_expires, part_mb_size, proxy):
    item = Haienv.select(venv_name)
    if not item:
        return {
            'success': 0,
            'msg': f'未找到名为{venv_name}的虚拟环境，当前虚拟环境目录为{os.environ.get("HAIENV_PATH", os.environ["HOME"])}，请用haienv list查看所有可用的虚拟环境，如需更改请设置环境变量HAIENV_PATH'
        }
    if item.extend == 'True':
        return {
            'success': 0,
            'msg': f'名为{venv_name}的虚拟环境为extend模式，暂不支持上传extend模式的venv'
        }
    result = await async_requests(RequestMethod.POST, f'{mars_url()}/ugc/update_cluster_venv?token={mars_token()}&venv_name={venv_name}&py={item.py}')
    remote_path = result['path']
    push_cmd = f"{sys.argv[0]} workspace push --list_timeout {list_timeout} --sync_timeout {sync_timeout} --cloud_connect_timeout {cloud_connect_timeout} \
        --token_expires {token_expires} --part_mb_size {part_mb_size} --file_type {FileType.ENV} \
        --env_provider {os.environ.get('CLOUD_STORAGE_PROVIDER', 'oss')} --env_local_path {item.path} --env_remote_path {remote_path}"
    if force:
        push_cmd += ' --force'
    if no_checksum:
        push_cmd += ' --no_checksum'
    if no_zip:
        push_cmd += ' --no_zip'
    if no_diff:
        push_cmd += ' --no_diff'
    if proxy:
        push_cmd += f' --proxy {proxy}'
    if os.system(push_cmd):
        return {
            'success': 0,
            'msg': '上传venv失败'
        }

    return {
        'success': 1,
        'msg': '上传venv成功'
    }
