
import aiofiles.os as asyncos
import os
import stat
import threading
import time
from concurrent.futures import wait
from datetime import datetime, timedelta, timezone
from typing import List
from functools import partial

from fastapi import Depends, HTTPException, Body
from fastapi_pagination import add_pagination, Page, Params

from .auth import *
from .audit import run_audit
from .utils import *
from .metrics import *
from api.app import app
from api.depends import get_api_user_with_name
from conf.utils import FileInfo, FileList, FileInfoList, FilePrivacy, DatasetType, \
    SyncDirection, SyncStatus, slice_bytes, hashkey, unzip_dir, tz_utc_8
from server_model.user import User


pod_id = os.environ.get('POD_NAME', 'POD_NAME-0').split('-')[-1]
worker_pools = WorkerPools()


# 启动时捞出running的任务，重新上传
@app.on_event("startup")
async def startup_event():
    # 启动audit任务
    logger.info('Run audit on start...')
    threading.Thread(target=run_audit, name='audit').start()

    logger.info('Recover tasks on start...')
    # keyformat: {PROVIDER}:{sync_from_cluster}:{index}:{param(:pod_id)/status/progress}
    try:
        keys = status_recorder.get_keys(f'{PROVIDER}:*:*:param:*')
        for k in keys:
            if k.split(':')[-1] != pod_id:
                # 只处理自己重启前的任务
                continue
            raw_param = status_recorder.get(k)
            if k.split(':')[1] == 'sync_to_cluster':
                if raw_param:
                    try:
                        param = ujson.loads(raw_param)
                        if param['file_list']:
                            param['file_list'] = FileList.parse_obj(param['file_list'])
                        logger.info(f'recovering sync_to_cluster task: {param}')
                        ret = await _sync_to_cluster_impl(**param, force=True)
                        logger.info(f'recovering sync_to_cluster task response: {ret}')
                    except Exception as e:
                        logger.error(f'recovering sync_to_cluster task failed: {str(e)}')
            if k.split(':')[1] == 'sync_from_cluster':
                if raw_param:
                    try:
                        param = ujson.loads(raw_param)
                        param['file_infos'] = FileInfoList.parse_obj(param['file_infos'])
                        logger.info(f'recovering sync_from_cluster task: {param}')
                        ret = await _sync_from_cluster_impl(**param, force=True)
                        logger.info(f'recovering sync_from_cluster task response: {ret}')
                    except Exception as e:
                        logger.error(f'recovering sync_from_cluster task failed: {str(e)}')
        logger.info('Recover tasks finished')
    except Exception as e:
        logger.error(f'Recover tasks failed: {str(e)}')


@app.get('/get_sts_token', dependencies=[Depends(validate_user_token)])
async def get_sts_token(username: str,
                        group: str,
                        name: str,
                        file_type: FileType,
                        file_privacy: Optional[FilePrivacy] = FilePrivacy.GROUP_SHARED,
                        dataset_type: Optional[DatasetType] = DatasetType.MINI,
                        ttl_seconds: Optional[int] = 1800):
    """
    获取sts token
    """
    try:
        bucket_name = CONF.cloud.storage.public_bucket if file_privacy == FilePrivacy.PUBLIC else CONF.cloud.storage.private_bucket
        _, cloud_base_path = get_base_path(username, group, name, file_type, file_privacy, dataset_type)
        resp = cloud_api.get_access_token(bucket_name, cloud_base_path, ttl_seconds)
    except Exception as e:
        logger.error(str(e))
        return {'success': 0, 'msg': str(e)}

    return {'success': 1, f'{PROVIDER}': resp}


@app.post('/list_bucket_files', dependencies=[Depends(validate_user_token)])
async def list_bucket_files(username,
                            group,
                            name,
                            file_type: FileType,
                            file_privacy: Optional[FilePrivacy] = FilePrivacy.GROUP_SHARED,
                            recursive: Optional[bool] = True):
    """
    @param username:  用户名
    @param group:     用户分组
    @param name:      工作区/env 名字
    @param file_type: 文件类型
    @param file_privacy: 文件权限
    @param recursive: 是否遍历全部子目录
    """
    try:
        _, cloud_base_path = get_base_path(username, group, name, file_type)
        logger.debug(f'收到list_bucket_files请求, base_path: {cloud_base_path}')
        bucket_name = get_bucket_name(file_type, file_privacy)
        files, _ = list_bucket_files_inner(cloud_base_path, bucket_name, recursive)
    except Exception as e:
        logger.error(str(e))
        raise HTTPException(status_code=500, detail=str(e))

    return files


@app.post('/list_cluster_files',
          dependencies=[Depends(validate_user_token)],
          response_model=Page[FileInfo])
async def list_cluster_files(username: str,
                             group: str,
                             name: str,
                             file_type: FileType,
                             no_checksum: bool = False,
                             no_hfignore:  Optional[bool] = False,
                             recursive: bool = True,
                             file_list: FileList = Body(...),
                             params: Params = Depends()):
    """
    @param username:  用户名
    @param group:     用户分组
    @param name:      工作区/env 名字
    @param file_type: 文件类型
    @param file_list: body, 用户工作区下文件列表
    """
    if file_type not in [FileType.WORKSPACE, FileType.ENV]:
        return {'success': 0, 'msg': f'不支持list {file_type}类型的文件'}
    try:
        cluster_base_path, _ = get_base_path(username, group, name, file_type)
        logger.debug(
            f'收到list_cluster_files请求, base_path: {cluster_base_path}, path: {file_list.files}, no_checksum: {no_checksum}, no_hfignore: {no_hfignore}'
        )
        return await paginate(cluster_base_path, file_list.files, no_checksum, no_hfignore,
                              recursive, params)
    except ClientException as ce:
        logger.error(f'list_cluster_files client error: {str(ce)}')
        raise HTTPException(status_code=400, detail=str(ce))
    except Exception as e:
        logger.error(f'list_cluster_files server error: {str(e)}')
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/sync_to_cluster', dependencies=[Depends(validate_user_token)])
async def sync_to_cluster(token: str,
                          username: str,
                          userid: str,
                          group: str,
                          name: str,
                          file_type: FileType,
                          file_privacy: Optional[FilePrivacy] = FilePrivacy.GROUP_SHARED,
                          dataset_type: Optional[DatasetType] = DatasetType.MINI,
                          no_zip: Optional[bool] = True,
                          file_list: FileList = Body(default=None)):
    """
    下载外部文件到集群内，目前不支持下载目录
    @param token:     mars token
    @param username:  用户名
    @param userid:    用户id
    @param group:     用户分组
    @param name:      工作区/env 名字
    @param file_type: 文件类型
    @param no_zip:    是否禁用文件压缩
    @param file_list: body, 用户工作区下文件列表
    """
    return await _sync_to_cluster_impl(token, username, userid, group, name, file_type, file_privacy, dataset_type, no_zip, file_list)


async def _sync_to_cluster_impl(token: str,
                                username: str,
                                userid: str,
                                group: str,
                                name: str,
                                file_type: FileType,
                                file_privacy: Optional[FilePrivacy],
                                dataset_type: Optional[DatasetType],
                                no_zip: bool,
                                file_list: FileList,
                                index: str = None,
                                force: bool = False):
    """
    注: 对dataset下载做了特判:
        - 校验时间戳以避免重复下载
        - 不修改filemode和owner
        - 不延期删除redis status key
    """
    user: User = await get_api_user_with_name(username)

    try:
        cluster_base_path, cloud_base_path = get_base_path(username, group, name, file_type, file_privacy, dataset_type)
    except Exception as e:
        if file_type in [FileType.WORKSPACE, FileType.ENV, FileType.DATASET]:
            with record_metrics('set_sync_status'):
                await user.aio_db.set_sync_status(file_type, name, SyncDirection.PUSH, SyncStatus.STAGE2_FAILED, '', '')
        logger.error(f'get_base_path error: {str(e)}')
        return {'success': 0, 'msg': str(e)}

    bucket_name = get_bucket_name(file_type, file_privacy)

    # 数据集下载到集群，可只指定name，无需指定file_list
    is_dataset = file_type == FileType.DATASET
    total_size = 0
    files = list()
    if file_list:
        files = file_list.files
    elif is_dataset:
        try:
            file_infos, downloaded_file_infos = await async_list_bucket_files_inner(cloud_base_path, bucket_name, recursive=True, diff=True, cluster_base_path=cluster_base_path)
            for fi in file_infos:
                total_size += fi.size
                files.append(fi.path)
            for downloaded_file_info in downloaded_file_infos:
                total_size += downloaded_file_info.size
                key = os.path.join(cloud_base_path, downloaded_file_info.path)
                await status_recorder.a_hset(status_key(index, 'progress', False), key, downloaded_file_info.size)
        except Exception as e:
            logger.error(f'list bucket error {str(e)}')
            return {'success': 0, 'msg': str(e)}

    if not index:
        index = hashkey(token, name, file_type, *files)
    logger.debug(f'hashkey for {name}, {file_type}, {files}: {index}')
    if len(files) == 0:
        await status_recorder.a_set(status_key(index, 'status', False), SyncPhase.FINISHED)
        return {'success': 1, 'msg': 'files already synced', 'index': index, 'dst_path': cluster_base_path}

    logger.info(f'开始下载远端{file_type} {files} 到本地 {cluster_base_path}' + f', 共{total_size}B' if total_size else '')

    if not force and await status_recorder.a_get(status_key(index, 'status', False)) == SyncPhase.RUNNING:
        msg = f'上一次同步 {files} 正在进行中, 忽略本次请求'
        logger.warning(msg)
        return {'success': 1, 'msg': msg, 'index': index, 'dst_path': cluster_base_path}

    # 将参数存档，方便重启后恢复
    index_info = {
        'token': token,
        'username': username,
        'userid': userid,
        'group': group,
        'name': name,
        'file_type': file_type,
        'file_privacy': file_privacy,
        'dataset_type': dataset_type,
        'no_zip': no_zip,
        'file_list': file_list.dict() if file_list else None,
        'index': index
    }
    await status_recorder.a_set(status_key(index, f'param:{pod_id}', False), ujson.dumps(index_info))

    if file_type in [FileType.WORKSPACE, FileType.ENV, FileType.DATASET]:
        with record_metrics('set_sync_status'):
            await user.aio_db.set_sync_status(file_type, name, SyncDirection.PUSH, SyncStatus.STAGE2_RUNNING, '', cluster_base_path)
    await status_recorder.a_set(status_key(index, 'status', False), SyncPhase.RUNNING)
    if is_dataset:
        await status_recorder.a_hset(status_key(index, 'progress', False), 'dataset_total_size', total_size)
    if not os.path.exists(cluster_base_path):
        os.makedirs(cluster_base_path)
        if not is_dataset:
            os.chown(cluster_base_path, int(userid), int(userid))
        # TODO: trim dataset on nfs

    subpath_set = set()
    futures = list()
    msg = ''
    for fname in files:
        key = os.path.join(cloud_base_path, fname)
        # 如为打包上传，临时放到工作区.hfai目录下，避免文件名冲突
        use_zip = not no_zip and fname.endswith('.zip')
        if use_zip:
            local_path = os.path.join(cluster_base_path, '.hfai', fname)
        else:
            local_path = os.path.join(cluster_base_path, fname)
        # 校验是否有非法路径
        check_is_subpath(cluster_base_path, local_path)
        dirname = os.path.dirname(local_path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
            subs = fname.split('/')
            subpath = cluster_base_path
            for sub in subs[:-1]:
                subpath += f'/{sub}'
                if subpath not in subpath_set and not is_dataset:
                    os.chown(subpath, int(userid), int(userid))
                    subpath_set.add(subpath)
        if not force and key in await status_recorder.a_get_hkeys(status_key(index, 'progress', False)):
            warn_msg = f'{key} 正在下载队列中, 忽略本次请求;'
            logger.warning(warn_msg)
            msg += warn_msg
            continue
        logger.debug(f'提交下载文件 {key}')
        loop = asyncio.get_running_loop()
        pool = worker_pools.get(index)
        func_call = partial(pool.submit,
                            resumable_download_with_retry,
                            bucket_name=bucket_name,
                            key=key,
                            filename=local_path,
                            file_type=file_type,
                            multiget_threshold=slice_bytes,
                            part_size=slice_bytes,
                            num_threads=4,
                            index=index,
                            username=username,
                            userid=userid,
                            use_zip=use_zip,
                            retries=10)
        future = await loop.run_in_executor(None, func_call)
        future.add_done_callback(download_callback)
        futures.append(future)
        RUNNING_TASKS_GAUGE.labels('push', username, file_type).inc()

    threading.Thread(target=wait_sync_to_cluster,
                     name=f'download-{index}',
                     args=(futures, index, user, file_type, name),
                     daemon=True).start()

    logger.info(f'提交同步任务成功, 文件列表: {files}')
    return {'success': 1, 'msg': '提交同步任务成功', 'index': index, 'dst_path': cluster_base_path}


def wait_sync_to_cluster(futures, index, user, file_type, name):
    # wait for complete
    logger.info(f'开始等待下载任务 {index}..')
    wait(futures)
    msg = ''
    for future in futures:
        e = future.exception()
        if e:
            msg += f'{str(e)};'
    if not msg:
        msg = SyncPhase.FINISHED
    logger.info(f'下载任务 {index} 完成: {msg}')
    worker_pools.finish(index)
    status_recorder.delete(status_key(index, f'param:{pod_id}', False))
    # 对于已结束任务，延期删除相应status key
    status_recorder.set(status_key(index, 'status', False), msg, expires=None if file_type == FileType.DATASET else 300)
    status_recorder.delete(status_key(index, 'progress', False))
    # 对于workspace/env类型，记录一下数据库
    if file_type in [FileType.WORKSPACE, FileType.ENV, FileType.DATASET]:
        status = SyncPhase.FINISHED if msg == SyncPhase.FINISHED else SyncStatus.STAGE2_FAILED
        with record_metrics('set_sync_status'):
            user.db.set_sync_status(file_type, name, SyncDirection.PUSH, status)


@app.post('/sync_from_cluster', dependencies=[Depends(validate_user_token)])
async def sync_from_cluster(token: str,
                            username: str,
                            group: str,
                            name: str,
                            file_type: str,
                            file_infos: FileInfoList,
                            file_privacy: Optional[FilePrivacy] = FilePrivacy.GROUP_SHARED,
                            dataset_type: Optional[DatasetType] = DatasetType.MINI):
    """
    上传集群内文件到外部
    @param token:     mars token
    @param username:  用户名
    @param group:     用户分组
    @param name:      工作区/env 名字
    @param file_type: 文件类型
    @param file_infos: body, 用户工作区下文件列表
    @param file_privacy: 文件权限
    """
    return await _sync_from_cluster_impl(token, username, group, name, file_type, file_infos, file_privacy, dataset_type)

async def _sync_from_cluster_impl(token: str,
                                  username: str,
                                  group: str,
                                  name: str,
                                  file_type: str,
                                  file_infos: FileInfoList,
                                  file_privacy: Optional[FilePrivacy] = FilePrivacy.GROUP_SHARED,
                                  dataset_type: Optional[DatasetType] = DatasetType.MINI,
                                  index: str = None,
                                  force: bool = False):
    try:
        cluster_base_path, cloud_base_path = get_base_path(
            username, group, name, file_type, file_privacy, dataset_type)
    except Exception as e:
        logger.error(f'get_base_path error: {str(e)}')
        return {'success': 0, 'msg': str(e)}

    # 文件相对目录
    file_list = [f.path for f in file_infos.files]
    if not index:
        index = hashkey(token, name, file_type, *file_list)
    logger.debug(f'hashkey for {name}, {file_type}, {file_list}: {index}')
    redis_status_key = status_key(index, 'status', True)
    if not force:
        phase = await status_recorder.a_get(redis_status_key)
        if phase in [SyncPhase.RUNNING, SyncPhase.INIT]:
            msg = f'上一次同步 {file_list} 正在进行中, 忽略本次请求'
            logger.warning(msg)
            return {'success': 1, 'msg': msg, 'index': index}

    # 将参数存档，方便重启后恢复
    index_info = {
        'token': token,
        'username': username,
        'group': group,
        'name': name,
        'file_type': file_type,
        'file_infos': file_infos.dict(),
        'file_privacy': file_privacy,
        'dataset_type': dataset_type,
        'index': index
    }
    await status_recorder.a_set(status_key(index, f'param:{pod_id}', True), ujson.dumps(index_info))

    quota_exceed = False
    user: User = await get_api_user_with_name(username)
    # upload_file_infos 为待上传文件信息列表
    upload_file_infos: List[FileInfo] = []
    try:
        for fi in file_infos.files:
            path = fi.path
            if '../' in path or '..\\' in path or path == '' or path.startswith('/'):
                raise Exception(f'不支持上传路径 "{path}"')

            if file_type in [FileType.WORKSPACE, FileType.ENV]:
                with record_metrics('set_sync_status'):
                    await user.aio_db.set_sync_status(file_type, name, SyncDirection.PULL, SyncStatus.STAGE1_RUNNING)
            await status_recorder.a_set(redis_status_key, SyncPhase.INIT)

            src_file = os.path.join(cluster_base_path, path)
            if fi.md5 is None or fi.size is None or fi.last_modified is None:
                # 如用户侧未指定md5，则重新计算
                logger.debug(f'list {src_file} 信息')
                files = await async_list_local_files_inner(cluster_base_path, path, False, True)
                if len(files) == 0:
                    logger.warning(f'未找到本地文件{src_file}, 可能被hfignore忽略')
                upload_file_infos.extend(files)
            else:
                upload_file_infos.append(fi)

        bucket_name = get_bucket_name(file_type, file_privacy)

        # 限制每天上传容量
        with record_metrics('get_usage_in_mb'):
            usage_in_mb = await user.downloaded_files.get_usage_in_mb()
        upload_size = sum([f.size for f in upload_file_infos])
        # 标识是否已经过滤已上传文件
        filtered = False
        # original_upload_file_infos 用来记录filter之前的文件列表，用于wait_sync_from_cluster在bucket上做gc
        original_upload_file_infos = upload_file_infos.copy()
        if upload_size > 1073741824:
            # 总上传文件大于1G时才过滤已上传文件，避免过度调用cloud api
            upload_file_infos = await filter_synced_files(bucket_name, index, cloud_base_path, upload_file_infos)
            upload_size = sum([f.size for f in upload_file_infos])
            filtered = True
        upload_mb = upload_size // 1024 // 1024
        with record_metrics('create_quota_df'):
            await user.quota.create_quota_df()
        limit = user.quota.cloud_storage_quota.download
        logger.debug(
            f'quota校验, request size: {upload_mb}MB, used size: {usage_in_mb}MB, limit size: {limit}MB'
        )
        quota_exceed = (usage_in_mb + upload_mb >= limit)
        if quota_exceed:
            msg = f'请联系管理员提升pull限额, 请求: {upload_mb}MB, 已用: {usage_in_mb}MB, 限额: {limit}MB'
            logger.error(msg)
            raise Exception(msg)
    except Exception as e:
        logger.error(str(e))
        await status_recorder.a_delete(status_key(index, f'param:{pod_id}', True))
        await status_recorder.a_set(redis_status_key, SyncPhase.FAILED)
        if file_type in [FileType.WORKSPACE, FileType.ENV]:
            with record_metrics('set_sync_status'):
                await user.aio_db.set_sync_status(file_type, name, SyncDirection.PULL, SyncStatus.STAGE1_FAILED)
        raise HTTPException(status_code=403 if quota_exceed else 500,
                            detail={
                                'success': 0,
                                'msg': str(e)
                            })

    futures = list()
    msg = ''
    src_files = [os.path.join(cluster_base_path, f.path) for f in upload_file_infos]
    dst_files = [os.path.join(cloud_base_path, f.path) for f in upload_file_infos]

    logger.info(f'开始上传本地目录 {src_files} 到远端，总共{upload_mb}MB...')
    await status_recorder.a_set(redis_status_key, SyncPhase.RUNNING)
    for i in range(len(src_files)):
        # 校验是否有非法路径
        check_is_subpath(cluster_base_path, src_files[i])
        # 校验是否已在上传队列
        if not force and dst_files[i] in await status_recorder.a_get_hkeys(status_key(index, 'progress', True)):
            warn_msg = f'{dst_files[i]} 正在上传队列中, 忽略本次请求;'
            logger.warning(warn_msg)
            msg += warn_msg
            continue

        logger.debug(f'提交上传文件 {src_files[i]}')

        # 小文件直接上传，大文件分片上传，阈值100MB
        loop = asyncio.get_running_loop()
        pool = worker_pools.get(index)
        func_call = partial(pool.submit,
                            resumable_upload_with_retry,
                            bucket_name=bucket_name,
                            key=dst_files[i],
                            filename=src_files[i],
                            multipart_threshold=slice_bytes,
                            part_size=slice_bytes,
                            num_threads=4,
                            index=index,
                            user=user,
                            file_type=file_type,
                            file_info=upload_file_infos[i],
                            filtered=filtered,
                            retries=10)
        future = await loop.run_in_executor(None, func_call)
        future.add_done_callback(upload_callback)
        futures.append(future)
        RUNNING_TASKS_GAUGE.labels('pull', username, file_type).inc()
        SYNCING_FILESIZE_GAUGE.labels('pull', username, file_type).inc(upload_file_infos[i].size)
    args = (futures, index, user, file_type, name, bucket_name, [f.path for f in original_upload_file_infos], cloud_base_path) if file_type in [FileType.DOC, FileType.PYPI] else (futures, index, user, file_type, name)
    threading.Thread(target=wait_sync_from_cluster,
                     name=f'upload-{index}',
                     args=args,
                     daemon=True).start()

    msg += f'提交同步任务成功, 文件列表: {dst_files}'
    logger.info(msg)
    return {'success': 1, 'msg': msg, 'index': index}


def wait_sync_from_cluster(futures, index, user, file_type, name, bucket_name=None, keys=None, cloud_base_path=None):
    # wait for complete
    logger.info(f'开始等待上传任务 {index}..')
    wait(futures)
    msg = ''
    for future in futures:
        e = future.exception()
        if e:
            msg += f'{str(e)};'

    # 对于doc类型，要删除bucket上过期文件
    if bucket_name and keys:
        logger.info('开始清理 bucket 过期目录')
        delete_condidates = []
        prefix = cloud_base_path + '/' if cloud_base_path else ''
        file_infos = cloud_api.list_bucket(bucket_name, prefix)[0]
        for obj in file_infos:
            if obj.path[len(prefix):] not in keys:
                delete_condidates.append(obj.path)
        for i in range(0, len(delete_condidates), 500):
            try:
                logger.debug(f'清理文件 {delete_condidates[i:i+500]}')
                batch_delete_objects_with_retry(bucket_name, delete_condidates[i:i+500])
            except Exception as e:
                msg += f'{str(e)};'
    if not msg:
        msg = SyncPhase.FINISHED

    logger.info(f'上传任务 {index} 完成: {msg}')
    worker_pools.finish(index)
    status_recorder.delete(status_key(index, f'param:{pod_id}', True))
    # 对于已结束任务，延期删除相应status key
    status_recorder.set(status_key(index, 'status', True), msg, expires=300)
    status_recorder.delete(status_key(index, 'progress', True))
    # 对于workspace/env类型，记录一下数据库
    if file_type in [FileType.WORKSPACE, FileType.ENV]:
        status = SyncPhase.FINISHED if msg == SyncPhase.FINISHED else SyncStatus.STAGE1_FAILED
        with record_metrics('set_sync_status'):
            user.db.set_sync_status(file_type, name, SyncDirection.PULL, status)


@app.post('/delete_files', dependencies=[Depends(validate_user_token)])
async def delete_files(username, group, name, file_type, file_list: FileList = Body(default=None)):
    if file_type not in [FileType.WORKSPACE, FileType.ENV]:
        return {'success': 0, 'msg': f'不支持删除{file_type}类型的文件'}
    delete_candidates = list()
    try:
        cluster_base_path, _ = get_base_path(username, group, name, file_type)
        if file_list is None or len(file_list.files) == 0:
            delete_candidates = [cluster_base_path]
        else:
            for f in file_list.files:
                if '..' in f:
                    raise ClientException(f'文件名中禁止包含父目录: {f}')
                delete_candidate = os.path.normpath(os.path.join(cluster_base_path, f.lstrip('/')))
                check_is_subpath(cluster_base_path, delete_candidate)
                delete_candidates.append(delete_candidate)

        logger.info(f'开始删除 {delete_candidates}')
        for f in delete_candidates:
            try:
                if os.path.isdir(f):
                    await rmtree(f)
                else:
                    await asyncos.remove(f)
                logger.info(f'删除子目录成功: {f}')
            except FileNotFoundError:
                logger.warning(f'{f} 不存在，跳过删除')
                pass
    except ClientException as ce:
        logger.error(f'删除失败 {delete_candidates}: {str(ce)}')
        raise HTTPException(status_code=400, detail=str(ce))
    except Exception as e:
        logger.error(f'删除失败 {delete_candidates}: {str(e)}')
        return {'success': 0, 'msg': str(e)}
    return {'success': 1}


def resumable_download_with_retry(bucket_name,
                                  key,
                                  filename,
                                  file_type,
                                  multiget_threshold=None,
                                  part_size=None,
                                  num_threads=None,
                                  index=None,
                                  username=None,
                                  userid=None,
                                  use_zip=None,
                                  retries=3):

    def percentage(consumed_bytes, total_bytes):
        if total_bytes:
            status_recorder.hset(status_key(index, 'progress', False), key, consumed_bytes)

    download_succeed = False
    is_dataset = file_type == FileType.DATASET
    for i in range(1, retries + 1):
        try:
            filemode = None
            tagging = dict()
            try:
                tagging = cloud_api.get_object_tagging(bucket_name, key)
                filemode = tagging.get('filemode', None)
            except Exception as e:
                logger.info(f'获取文件tagging {key}失败, 忽略: {str(e)}')

            logger.info(f'开始下载 {key}')
            if not download_succeed:
                cloud_api.resumable_download(bucket_name, key, filename, multiget_threshold,
                                             part_size, percentage, num_threads)
            download_succeed = True
            logger.info(f'下载 {key} 完成')
            if not is_dataset:
                os.chown(filename, int(userid), int(userid))
                # 尝试从tag中恢复文件filemode
                if filemode:
                    os.chmod(filename, int(filemode, 8))

        except Exception as e:
            if i == retries:
                raise Exception({'key': key, 'size': 0, 'username': username, 'file_type': file_type, 'msg': str(e)})
            else:
                logger.info(f'第{i}次下载{key}失败: {str(e)}, 尝试重试...')
                time.sleep(1)
                continue

    size = os.path.getsize(filename)
    if use_zip:
        dirname = os.path.dirname(filename).split('/.hfai')[0]
        logger.info(f'开始解压 {filename}')
        try:
            unzippped_files = unzip_dir(filename, dirname)
            if not is_dataset:
                for unzipped_file in unzippped_files:
                    f_path = os.path.join(dirname, unzipped_file)
                    os.chown(f_path, int(userid), int(userid))
        except FileNotFoundError as fe:
            if fe.filename and fe.filename.endswith('cap_bin'):
                logger.info(f'忽略cap_bin目录错误')
            else:
                raise Exception({'key': key, 'size': size, 'username': username, 'file_type': file_type, 'msg': str(fe)})
        except Exception as e:
            logger.info(f'解压文件{filename}失败： {str(e)}')
            raise Exception({'key': key, 'size': size, 'username': username, 'file_type': file_type, 'msg': str(e)})
        finally:
            if os.path.exists(filename):
                os.remove(filename)

    if is_dataset and download_succeed:
        try:
            # 标记dataset回收时间为7d
            expire_at = (datetime.utcnow().replace(
                tzinfo=timezone.utc).astimezone(tz_utc_8) +
                        timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
            tagging['expire_at'] = expire_at
            cloud_api.set_object_tagging(bucket_name, key, tagging)
        except Exception as e:
            logger.info(f'记录dataset {key} tagging expire_at error: {str(e)}')
            pass

    return {'key': key, 'size': size, 'username': username, 'file_type': file_type}


def resumable_upload_with_retry(bucket_name,
                                key,
                                filename,
                                multipart_threshold=None,
                                part_size=None,
                                num_threads=None,
                                index=None,
                                user=None,
                                file_type=None,
                                file_info=None,
                                filtered=False,
                                retries=3):

    def percentage(consumed_bytes, total_bytes):
        if total_bytes:
            status_recorder.hset(status_key(index, 'progress', True), key, consumed_bytes)

    upload_succeed = False
    for i in range(1, retries + 1):
        try:
            if not filtered:
                # 校验云端是否有，避免打断情况下导致的重复上传，浪费带宽资源
                try:
                    tagging = cloud_api.get_object_tagging(bucket_name, key)
                    md5 = tagging.get('md5', None)
                    if md5 == file_info.md5:
                        logger.info(f'  {filename} 之前已上传, md5: {md5}, 跳过')
                        status_recorder.hset(status_key(index, 'progress', True), key, file_info.size)
                        return {'key': key, 'size': file_info.size, 'username': user.user_name, 'file_type': file_type}
                except Exception as e:
                    pass

            # TODO: 文件扫描，校验文件类型，禁止非法文件
            if not upload_succeed:
                # 记录数据库
                with record_metrics('insert_downloaded_file'):
                    user.db.insert_downloaded_file(file_type, filename, file_info.size,
                                                   file_info.last_modified, file_info.md5,
                                                   SyncStatus.RUNNING)
                src_file_mode = oct(stat.S_IMODE(os.lstat(filename).st_mode))
                tagging = f'size={file_info.size}&md5={file_info.md5}&source=cluster&filemode={src_file_mode}'
                logger.info(f'开始上传 {key}')
                cloud_api.resumable_upload(bucket_name, key, filename, multipart_threshold,
                                           part_size, percentage, num_threads, tagging)
                logger.info(f'上传 {key} 完成')
                upload_succeed = True
            with record_metrics('update_downloaded_file_status'):
                user.db.update_downloaded_file_status(filename, file_info.md5, SyncStatus.FINISHED)
            return {'key': key, 'size': file_info.size, 'username': user.user_name, 'file_type': file_type}
        except Exception as e:
            if i == retries:
                logger.info(f'上传 {filename} 失败')
                with record_metrics('update_downloaded_file_status'):
                    user.db.update_downloaded_file_status(filename, file_info.md5, SyncStatus.FAILED)
                raise Exception({'key': key, 'size': file_info.size, 'username': user.user_name, 'file_type': file_type, 'msg': str(e)})
            else:
                logger.info(f'第{i}次上传{key}失败: {str(e)}, 尝试重试...')
                time.sleep(1)
                continue


def download_callback(future):
    try:
        rst = future.result()
        logger.debug(f'download callback discard {rst["key"]}')
        SYNCED_FILESIZE_COUNTER.labels('push', rst["username"], rst["file_type"]).inc(rst['size'])
        SYNCED_FILENUM_COUNTER.labels('push', rst["username"], rst["file_type"]).inc()
    except Exception as e:
        rst = e.args[0]
        logger.debug(f'download callback exception with key {rst["key"]}: {str(e)}')
        Failed_TASKS_COUNTER.labels('push', rst["username"], rst["file_type"]).inc()
    finally:
        RUNNING_TASKS_GAUGE.labels('push', rst["username"], rst["file_type"]).dec()


def upload_callback(future):
    try:
        rst = future.result()
        logger.debug(f'upload callback discard {rst["key"]}')
        SYNCED_FILESIZE_COUNTER.labels('pull', rst["username"], rst["file_type"]).inc(rst['size'])
        SYNCED_FILENUM_COUNTER.labels('pull', rst["username"], rst["file_type"]).inc()
    except Exception as e:
        rst = e.args[0]
        logger.debug(f'upload callback exception with key {rst["key"]}: {str(e)}')
        Failed_TASKS_COUNTER.labels('pull', rst["username"], rst["file_type"]).inc()
    finally:
        RUNNING_TASKS_GAUGE.labels('pull', rst["username"], rst["file_type"]).dec()
        SYNCING_FILESIZE_GAUGE.labels('pull', rst["username"], rst["file_type"]).dec(rst['size'])


def batch_delete_objects_with_retry(bucket_name, files, retries=3):
    for i in range(1, retries + 1):
        try:
            cloud_api.batch_delete_objects(bucket_name, files)
            return
        except Exception as e:
            if i == retries:
                msg = f'删除bucket {files}失败'
                logger.info(msg)
                raise Exception(msg)
            else:
                logger.info(f'第{i}次删除bucket {files}失败: {str(e)}, 尝试重试...')
                continue


######################### status api #########################

@app.get('/sync_to_cluster/status',
         dependencies=[Depends(validate_user_token)])
async def sync_to_cluster_status(index):
    status = 'none'
    msg = index
    total = None
    phase = await status_recorder.a_get(status_key(index, 'status', False))
    if phase is not None:
        if phase == SyncPhase.RUNNING:
            status = SyncPhase.RUNNING
            progress = await status_recorder.a_get_hvalues(status_key(index, 'progress', False))
            msg = sum(progress.values())
            if 'dataset_total_size' in progress.keys():
                total = int(progress['dataset_total_size'])
                msg -= total
        elif phase == SyncPhase.FINISHED:
            status, msg = SyncPhase.FINISHED, ''
        else:
            status, msg = SyncPhase.FAILED, phase
    logger.debug(
        f'---- sync_to_cluster_status: {index[:10]} {status} {msg} ----')
    if status == 'none':
        raise HTTPException(status_code=400, detail='不存在的index')
    ret = {'success': 1, 'status': status, 'progress': msg, 'total': total} if total else {'success': 1, 'status': status, 'msg': msg}

    return ret


@app.get('/sync_from_cluster/status',
         dependencies=[Depends(validate_user_token)])
async def sync_from_cluster_status(index):
    status = 'none'
    msg = index
    phase = await status_recorder.a_get(status_key(index, 'status', True))
    if phase is not None:
        if phase == SyncPhase.RUNNING:
            status = SyncPhase.RUNNING
            progress = await status_recorder.a_get_hvalues(status_key(index, 'progress', True))
            msg = sum(progress.values())
        elif phase == SyncPhase.INIT:
            status, msg = SyncPhase.INIT, ''
        elif phase == SyncPhase.FINISHED:
            status, msg = SyncPhase.FINISHED, ''
        else:
            status, msg = SyncPhase.FAILED, phase
    logger.debug(
        f'---- sync_from_cluster_status: {index[:10]} {status} {msg} ----')
    if status == 'none':
        raise HTTPException(status_code=400, detail='不存在的index')
    return {'success': 1, 'status': status, 'msg': msg}


@app.post("/token", response_model=Token)
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(
        allowed_users, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    # 暂时不设置过期时间
    access_token = create_access_token(data={"sub": user.username})
    return {"success": 1, "access_token": access_token, "token_type": "bearer"}


@app.get('/dump')
async def dump():
    """
    debug用
    """
    msg = ''
    for name, pool in worker_pools.pools.items():
        msg += f'Name: {name},\n\tProcesses: {pool._processes}\n\tTotal tasks: {pool._queue_count}\n'
        if len(pool._pending_work_items) > 0:
            msg += f'\tPending tasks:\n'
        for k, v in pool._pending_work_items.items():
            msg += f'\t\ttask_id: {k}, is_running: {v.future.running()}, func_name: {v.fn.__name__}, args: {v.args}, kwargs: {v.kwargs}'.replace('\n', ', ') + '\n'

    return msg


@app.on_event("shutdown")
async def shutdown_event():
    logger.info(f'Shutdown...')


add_pagination(app)
