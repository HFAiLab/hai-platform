
import asyncio
import functools
import ujson
import os
import shutil
from cachetools import TTLCache
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from multiprocessing import get_context
from time import sleep
from random import randint
from enum import Enum
from pathlib import Path
from typing import Optional, List, Callable

from fastapi import HTTPException
from fastapi_pagination.api import create_page, resolve_params
from fastapi_pagination.bases import AbstractPage, AbstractParams

from conf import CONF
from conf.utils import FileInfo, FileType, FilePrivacy, DatasetType, list_local_files_inner, hashkey
from db import a_redis, redis_conn
from utils import asyncwrap
from .metrics import DB_FAILURE_COUNTER
from logm import logger
from .provider import OSSApi, MockApi


# 集群内访问外网的proxy
try:
    proxies = { 'http': CONF.cloud.storage.service.proxy_endpoint, 'https': CONF.cloud.storage.service.proxy_endpoint }
except:
    proxies = None

PROVIDER = CONF.cloud.storage.provider
if PROVIDER == 'oss':
    cloud_api = OSSApi(CONF.cloud.storage.endpoint,
                       CONF.cloud.storage.access_key_id,
                       CONF.cloud.storage.access_key_secret,
                       uid=CONF.cloud.storage.uid,
                       role_arn=CONF.cloud.storage.role_arn,
                       breakpoint_info_path=CONF.cloud.storage.service.breakpoint_info_path,
                       proxies=proxies)
else:
    cloud_api = MockApi()


class WorkerPools:
    def __init__(self, method='spawn'):
        self.init_worker_num = int(os.environ.get('WORKERS', 4))
        self.shared_worker_num = self.init_worker_num * 4
        self.context = get_context(method)
        self.pools = dict()
        self.max_pools = 10

    def get(self, name):
        if name not in self.pools.keys():
            # 超出max_pools的任务放到共享pool里运行
            if len(self.pools) > self.max_pools or name == 'shared':
                if 'shared' not in self.pools.keys():
                    self._create('shared', True)
                return self.pools['shared']
            self._create(name)
        return self.pools[name]

    def _create(self, name, shared=False):
        worker_num = self.shared_worker_num if shared else self.init_worker_num
        logger.info(f'creating process workers, name: {name}, worker_num: {worker_num}')
        self.pools[name] = ProcessPoolExecutor(max_workers=worker_num, mp_context=self.context)
        logger.info(f'create process workers successful, name: {name}, worker_num: {worker_num}')

    def finish(self, name):
        if name not in self.pools.keys():
            return
        logger.info(f'shutting down process workers, name: {name}')
        self.pools[name].shutdown()
        logger.info(f'shutdown process workers successful, name: {name}')
        self.pools.pop(name, None)


class ClientException(Exception):
    pass

class SyncPhase(str, Enum):
    '''
    server同步云端的临时状态
    '''
    INIT = 'init'
    RUNNING = 'running'
    FINISHED = 'finished'
    FAILED = 'failed'


def retry_on_exception(func: Callable, *args, **kwargs):
    retries = 3
    for i in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if i == retries:
                msg = f"Too many retries {retries} of {func.__name__}"
                print(msg)
                raise Exception(msg) from e
            else:
                print(f"attempt {i} retry of {func.__name__} failed: {str(e)}")
                sleep(randint(1, 200)/100)
                continue


async def a_retry_on_exception(func: Callable, *args, **kwargs):
    retries = 3
    for i in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if i == retries:
                msg = f"Too many retries {retries} of {func.__name__}"
                print(msg)
                raise Exception(msg) from e
            else:
                print(f"attempt {i} retry of {func.__name__} failed: {str(e)}")
                sleep(randint(1, 200)/100)
                continue


@contextmanager
def record_metrics(operation):
    try:
        yield
    except Exception as e:
        DB_FAILURE_COUNTER.labels(operation).inc()
        raise Exception(f'record operation error: {operation}, {str(e)}')


def metrics_wrapper(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with record_metrics(func.__name__):
            return retry_on_exception(func, *args, **kwargs)

    @functools.wraps(func)
    async def a_wrapper(*args, **kwargs):
        with record_metrics(func.__name__):
            return await a_retry_on_exception(func, *args, **kwargs)

    if asyncio.iscoroutinefunction(func):
        return a_wrapper
    return wrapper


class StatusRecorder:
    def __init__(self, recorder, aio_recorder):
        self.recorder = recorder
        self.aio_recorder = aio_recorder

    @metrics_wrapper
    async def a_get(self, key):
        val = await self.aio_recorder.get(key)
        if val:
            return val.decode()
        return None

    @metrics_wrapper
    async def a_get_hvalues(self, name):
        '''
        获取hash中的所有key/value
        '''
        members = await self.aio_recorder.hgetall(name)
        ret = {k.decode():int(v.decode()) for k, v in members.items()}
        return ret

    @metrics_wrapper
    async def a_get_hkeys(self, name):
        '''
        获取hash中的所有key
        '''
        members = await self.aio_recorder.hkeys(name)
        ret = [m.decode() for m in members]
        return ret

    @metrics_wrapper
    async def a_hset(self, name, key, value):
        await self.aio_recorder.hset(name, key, value)

    @metrics_wrapper
    async def a_set(self, key, value, expires=604800):
        await self.aio_recorder.set(key, value, expires)

    @metrics_wrapper
    async def a_expire(self, key, expires=604800):
        await self.aio_recorder.expire(key, expires)

    @metrics_wrapper
    async def a_exists(self, key):
        ret = await self.aio_recorder.exists(key)
        return ret > 0

    @metrics_wrapper
    async def a_delete(self, key):
        await self.aio_recorder.delete(key)

    @metrics_wrapper
    def get(self, key):
        val = self.recorder.get(key)
        if val:
            return val.decode()
        return val

    @metrics_wrapper
    def get_keys(self, key):
        keys = self.recorder.keys(key)
        return [k.decode() for k in keys]

    @metrics_wrapper
    def set(self, key, value, expires=604800):
        self.recorder.set(key, value, expires)

    @metrics_wrapper
    def hset(self, name, key, value):
        self.recorder.hset(name, key, value)

    @metrics_wrapper
    def delete(self, key):
        self.recorder.delete(key)


status_recorder = StatusRecorder(redis_conn, a_redis)


def status_key(index, key, is_upload: bool):
    top_key = 'sync_from_cluster' if is_upload else 'sync_to_cluster'
    return f'{PROVIDER}:{top_key}:{index}:{key}'


def get_bucket_name(file_type, file_privacy = FilePrivacy.GROUP_SHARED):
    if file_privacy == FilePrivacy.PUBLIC:
        if file_type == FileType.DOC:
            bucket_name = CONF.cloud.storage.doc_bucket
        elif file_type == FileType.PYPI:
            bucket_name = CONF.cloud.storage.pypi_bucket
        elif file_type == FileType.DATASET:
            bucket_name = CONF.cloud.storage.public_bucket
        elif file_type == FileType.WEBSITE:
            bucket_name = CONF.cloud.storage.official_website_bucket
        else:
            msg = f'{file_type}不允许为{file_privacy}类型'
            logger.error(msg)
            raise HTTPException(status_code=403, detail={'success': 0, 'msg': msg})
    else:
        if file_type in [FileType.DOC, FileType.PYPI, FileType.WEBSITE]:
            msg = f'{file_type}不允许为{file_privacy}类型'
            logger.error(msg)
            raise HTTPException(status_code=403, detail={'success': 0, 'msg': msg})
        bucket_name = CONF.cloud.storage.private_bucket
    return bucket_name


def list_bucket_files_inner(prefix, bucket_name, recursive=False, diff=False, cluster_base_path=None):
    prefix = prefix.replace('//', '/').replace('\\', '/')
    if not prefix.endswith('/'):
        prefix += '/'
    
    files = []
    downloaded_files = []
    logger.debug(f'list bucket files in {bucket_name}, prefix: {prefix}, recursive: {recursive}')
    file_infos = cloud_api.list_bucket(bucket_name, prefix, recursive)[0]
    for fi in file_infos:
        if fi.path[-1] == '/':
            logger.debug(f'skip dir {fi.path}')
            continue
        ts = fi.last_modified
        key = fi.path[len(prefix):]
        if key == '':
            continue
        f = FileInfo(path=key, size=fi.size, last_modified=ts)
        if diff and cluster_base_path:
            local_path = os.path.join(cluster_base_path, key)
            if os.path.exists(local_path) and os.path.getmtime(local_path) > float(ts):
                logger.info(f'{local_path} 之前已下载, 跳过')
                downloaded_files.append(f)
                continue
        files.append(f)

    logger.debug(f'list bucket files in {bucket_name}, prefix: {prefix}, diff: {diff}, got {len(files)} items, {len(downloaded_files)} downloaded items')
    return files, downloaded_files


@asyncwrap
def filter_synced_files(bucket_name, index, prefix, upload_file_infos):
    filtered_file_infos = list()
    for i in range(len(upload_file_infos)):
        try:
            file_info = upload_file_infos[i]
            key = os.path.join(prefix, file_info.path)
            tagging = cloud_api.get_object_tagging(bucket_name, key)
            md5 = tagging.get('md5', None)
            if md5 == file_info.md5:
                logger.info(f'  {key} 之前已上传, md5: {md5}, 跳过')
                status_recorder.hset(status_key(index, 'progress', True), key, file_info.size)
                continue
        except Exception as e:
            logger.error(f'filter_synced_files {key} error: {str(e)}')

        filtered_file_infos.append(upload_file_infos[i])
    return filtered_file_infos


rmtree = asyncwrap(shutil.rmtree)

async_list_bucket_files_inner = asyncwrap(list_bucket_files_inner)

async_list_local_files_inner = asyncwrap(list_local_files_inner)

### paginate
ttl_seconds = 30
cache = TTLCache(maxsize=10000, ttl=ttl_seconds)

async def get_files_cache(key, base_path, path_list, no_checksum, no_hfignore, recursive):
    redis_exists = True
    # 先尝试从本地缓存取
    cache_files = cache.pop(key, None)
    if cache_files:
        # 首先检查是否redis中存在key，如不存在，则不应返回本地cache
        redis_exists = await status_recorder.a_exists(key)
        if redis_exists:
            # 重新赋值以renew cache的ttl
            cache.setdefault(key, cache_files)
            return cache_files
    # 再从redis取
    if redis_exists:
        raw = await status_recorder.a_get(key)
        if raw:
            files = ujson.loads(raw)
            cache.setdefault(key, files)
            return files
    # 最后list目录
    files = []
    print(f'hashkey for {base_path}, {path_list}, {no_checksum}, {no_hfignore}, {recursive}: {key}')
    for path in path_list:
        check_is_subpath(base_path, os.path.abspath(f'{base_path}/{path}'))
        f = await async_list_local_files_inner(base_path, path, no_checksum, no_hfignore, recursive, True)
        files += f
    data = [f.dict() for f in files]
    await status_recorder.a_set(key, ujson.dumps(data), ttl_seconds)
    cache.setdefault(key, files)

    return files


async def paginate(
    base_path: str,
    path_list: List[str],
    no_checksum: bool,
    no_hfignore: bool,
    recursive: bool,
    params: Optional[AbstractParams] = None,
) -> AbstractPage:
    raw_params = resolve_params(params).to_raw_params()
    path_list.sort()
    key = f"{PROVIDER}:file_cache:{hashkey(base_path, *path_list, str(no_checksum), str(no_hfignore), str(recursive))}"
    try:
        files = await get_files_cache(key, base_path, path_list, no_checksum, no_hfignore, recursive)
    except Exception:
        await status_recorder.a_delete(key)
        cache.pop(key, None)
        raise
    total = len(files)
    start = raw_params.offset
    end = raw_params.offset + raw_params.limit
    # 默认每页100条，每隔5次查询检查一次文件是否被删除
    if start % 500 == 0 and start < total:
        if isinstance(files[start], dict) and not os.path.exists(os.path.join(base_path, files[start]["path"])):
            await status_recorder.a_delete(key)
            cache.pop(key, None)
            raise ClientException(f'{files[start]["path"]}不存在, 可能正在被删除, 请重试当前操作')

    if end >= total:
        await status_recorder.a_delete(key)
        cache.pop(key, None)
    else:
        # 分页接口查询，renew redis缓存有效期
        await status_recorder.a_expire(key, ttl_seconds)
    return create_page(items=files[start: end], total=total, params=params)


def check_is_subpath(basepath, subpath):
    '''
    校验subpath指向文件/目录是否在basepath内
    '''
    basepath = os.path.normpath(basepath)
    subpath = os.path.normpath(subpath)
    if basepath != subpath and Path(basepath).resolve() not in Path(subpath).resolve().parents:
        raise ClientException(f"目的路径 {subpath} 超出限定范围，非法！")


def get_base_path(username, group, name, file_type, file_privacy: FilePrivacy = FilePrivacy.GROUP_SHARED, dataset_type: DatasetType = DatasetType.MINI):
    '''
    根据用户信息获取相应文件类型的集群根目录
    '''
    if file_privacy == FilePrivacy.PUBLIC:
        # 允许上传到public bucket的用户
        public_bucket_allowed_users = CONF.cloud.storage.service.public_bucket_allowed_users.split(',')
        if username not in public_bucket_allowed_users:
            raise ClientException(f'{username} 禁止上传到公共bucket!')

    if not (group and username and name):
        raise ClientException('必须指定workspace_base_path/group/username/name')
    if '/' in name:
        raise ClientException('name格式非法')
    if file_privacy == FilePrivacy.PUBLIC and file_type not in [FileType.DOC, FileType.PYPI, FileType.DATASET, FileType.WEBSITE]:
        raise ClientException(f'{file_type}不允许为{file_privacy}类型')
    if file_privacy != FilePrivacy.PUBLIC and file_type in [FileType.DOC, FileType.PYPI, FileType.WEBSITE]:
        raise ClientException(f'{file_type}不允许为{file_privacy}类型')

    if file_type == FileType.WORKSPACE:
        # 本地工作区根目录
        workspace_base_path = CONF.cloud.storage.service.workspace_path
        cluster_base_path = f'{workspace_base_path}/{group}/{username}/workspaces/{name}'
        cloud_base_path = f'{group}/{username}/workspaces/{name}'
        check_is_subpath(workspace_base_path, cluster_base_path)
    elif file_type == FileType.ENV:
        # 本地venv根目录
        env_base_path = CONF.cloud.storage.service.env_path
        cluster_base_path = f'{env_base_path}/{group}/shared/hfai_envs/{username}/{name}'
        cloud_base_path = f'{group}/shared/hfai_envs/{username}/{name}'
        check_is_subpath(env_base_path, cluster_base_path)
    elif file_type == FileType.DATASET:
        # 本地数据集根目录, public
        public_dataset_base_path = CONF.cloud.storage.service.public_dataset_path
        # 本地数据集根目录, group_shared/private
        private_dataset_base_path = CONF.cloud.storage.service.private_dataset_path
        if file_privacy == FilePrivacy.PRIVATE:
            cluster_base_path = f'{private_dataset_base_path}/{group}/{username}/{dataset_type}/{name}'
            check_is_subpath(private_dataset_base_path, cluster_base_path)
            cloud_base_path = f'datasets/{group}/{username}/{dataset_type}/{name}'
        if file_privacy == FilePrivacy.GROUP_SHARED:
            cluster_base_path = f'{private_dataset_base_path}/{group}/{dataset_type}/{name}'
            check_is_subpath(private_dataset_base_path, cluster_base_path)
            cloud_base_path = f'datasets/{group}/{dataset_type}/{name}'
        if file_privacy == FilePrivacy.PUBLIC:
            cluster_base_path = f'{public_dataset_base_path}/{dataset_type}/{name}'
            check_is_subpath(public_dataset_base_path, cluster_base_path)
            cloud_base_path = f'datasets/public/{dataset_type}/{name}'
    elif file_type == FileType.DOC:
        # hfai doc根目录
        doc_base_path = CONF.cloud.storage.service.doc_path
        cluster_base_path = doc_base_path
        cloud_base_path = ''
    elif file_type == FileType.PYPI:
        # hfai pypi包根目录
        pypi_base_path = CONF.cloud.storage.service.pypi_path
        cluster_base_path = pypi_base_path
        cloud_base_path = 'simple'
    elif file_type == FileType.WEBSITE:
        # hfai 官网demo根目录
        official_website_base_path = CONF.cloud.storage.service.official_website_path
        cluster_base_path = f'{official_website_base_path}/{name}'
        check_is_subpath(official_website_base_path, cluster_base_path)
        cloud_base_path = f'examples/{name}'
    else:
        raise ClientException(f'非法文件类型 {file_type}')

    if '..' in cluster_base_path or '..' in cloud_base_path:
        raise ClientException(f'请求中禁止包含父目录, cluster_base_path: {cluster_base_path}, cloud_base_path: {cloud_base_path}')
    return cluster_base_path, cloud_base_path
