import os
import stat
from asyncio import sleep
from datetime import datetime, timedelta, timezone
from typing import List

from rich.box import ASCII2
from rich.console import Console
from rich.table import Table
from rich.progress import Progress

# client/api/api_config.py
from .api_config import get_mars_token as mars_token
from .api_config import get_mars_url as mars_url
# client/api/api_utils.py
from .api_utils import async_requests, RequestMethod
# conf/utils.py
from .utils import FileType, SyncDirection, SyncStatus, FileInfo, FileList, FileInfoList, bytes_to_human, \
    get_file_info, list_local_files_inner, hashkey, zip_dir, tz_utc_8
# cloud_storage/provider
from .provider import OSSApi, MockApi

from itertools import chain


def print_bold(text):
    print(f'\33[1m{text}\33[0m')


async def diff_local_cluster(local_path: str,
                             name: str,
                             file_type: str,
                             subpath_list: List[str],
                             no_checksum: bool = False,
                             no_hfignore: bool = False,
                             no_diff: bool = False,
                             exclude_list: List = [],
                             timeout: int = 3600,
                             is_push: bool = False):
    """
    比较本地和远端目录diff
    @param local_path: 本地工作区目录
    @param name: 工作区或env的名字
    @param file_type: 类型
    @param subpath_list: diff的子目录
    @param no_checksum: 是否禁用checksum
    @param no_hfignore: 是否忽略hfignore
    @param no_diff: 是否禁用差量上传
    @param exclude_list: 排除在外的路径
    @return local_only_files       list[FileInfo]: 本地未上传文件
            cluster_only_files     list[FileInfo]：远端未下载文件
            local_changed_files    list[FileInfo]: 本地与远端有差异文件（本地角度）
            cluster_changed_files  list[FileInfo]: 本地与远端有差异文件（远端角度）
    """
    local_only_files, cluster_only_files, local_changed_files, cluster_changed_files = [], [], [], []
    print_bold(f'开始遍历本地{file_type}目录...')
    local_files = list(chain(*[list_local_files_inner(local_path, path, no_checksum, no_hfignore) for path in subpath_list]))
    if no_diff:
        return local_files, cluster_only_files, local_changed_files, cluster_changed_files
    print(f'-> 本地{file_type}文件数: {len(local_files)}, 文件大小：{bytes_to_human(sum([f.size for f in local_files]))}')
    if is_push and len(local_files) > 10000:
        print('Tips: 如集群侧文件数过多, push时可增加 --no_diff 参数以跳过耗时较长的集群目录遍历, 集群侧如有同名文件将被覆盖')
    print_bold(f'开始遍历集群{file_type}目录...')
    cluster_files = await list_cluster_files(name, file_type, subpath_list,
                                             no_checksum, no_hfignore, timeout)
    print(f'-> 集群{file_type}文件数: {len(cluster_files)}, 文件大小：{bytes_to_human(sum([f.size for f in cluster_files]))}')
    local_files = [l_file for l_file in local_files if l_file.path not in exclude_list]
    cluster_files = [c_file for c_file in cluster_files if c_file.path not in exclude_list]
    local_files_map = {l_file.path: l_file for l_file in local_files}
    cluster_files_map = {c_file.path: c_file for c_file in cluster_files}
    for local_file in local_files:
        if local_file.path not in cluster_files_map:
            local_only_files.append(local_file)
        else:
            if local_file.md5 != cluster_files_map[local_file.path].md5 or \
                    local_file.size != cluster_files_map[local_file.path].size:
                local_changed_files.append(local_file)
                cluster_changed_files.append(cluster_files_map[local_file.path])
    for cluster_file in cluster_files:
        if cluster_file.path not in local_files_map:
            cluster_only_files.append(cluster_file)
    return local_only_files, cluster_only_files, local_changed_files, cluster_changed_files


def print_diff(local_only_files,
               cluster_only_files,
               changed_files,
               focus_list=['本地', '集群']):
    console = Console()
    table = Table(box=ASCII2, style='dim', show_header=False)
    for column in ['type', 'filename']:
        table.add_column(column)
    for note, files in zip(
        ["[green]本地未上传文件", "[yellow]集群未下载文件", "[red]本地与集群有差异文件"],
        [local_only_files, cluster_only_files, changed_files]):
        if any([word in note for word in focus_list]):
            table.add_row(
                note,
                '\t'.join([f'{f.path}({bytes_to_human(f.size)})'
                           for f in files]) if files else 'None',
                end_section=True)
    console.print(table)


############################################ 访问 server #################################################


async def get_sts_token(provider, name, file_type, token_expires, **kwargs):
    """
    获取云端存储的临时token
    """
    token = kwargs.get('token', mars_token())
    timeout = 60
    url = f'{mars_url()}/ugc/get_sts_token?token={token}&name={name}&file_type={file_type}&ttl_seconds={token_expires}'
    result = await async_requests(RequestMethod.POST,
                                  url,
                                  retries=3,
                                  timeout=timeout)
    if provider not in result:
        raise Exception(f'get_sts_token returns non {provider} data: {result}')
    return result[provider]


async def set_sync_status(file_type: FileType, workspace_name,
                          direction: SyncDirection, status: SyncStatus,
                          local_path, cluster_path, **kwargs):
    token = kwargs.get('token', mars_token())
    timeout = 60
    url = f'{mars_url()}/ugc/set_sync_status?token={token}&file_type={file_type}&name={workspace_name}&direction={direction}&status={status}&local_path={local_path}&cluster_path={cluster_path}'
    await async_requests(RequestMethod.POST, url, retries=3, timeout=timeout)
    return


async def get_sync_status(file_type: FileType, workspace_name='*', **kwargs):
    token = kwargs.get('token', mars_token())
    timeout = 60
    url = f'{mars_url()}/ugc/get_sync_status?token={token}&file_type={file_type}&name={workspace_name}'
    result = await async_requests(RequestMethod.POST,
                                  url,
                                  retries=3,
                                  timeout=timeout)
    return result['data']


async def delete_workspace(workspace_name, files, **kwargs):
    token = kwargs.get('token', mars_token())
    timeout = 60
    url = f'{mars_url()}/ugc/delete_files?token={token}&name={workspace_name}&file_type={FileType.WORKSPACE}'
    file_list = FileList(files=list(files))
    data = f'{{"file_list": {file_list.json()}}}'
    await async_requests(RequestMethod.POST, url, retries=3, data=data, timeout=timeout)
    return


async def list_cluster_files(name: str, file_type: str, subpaths: List[str],
                             no_checksum: bool, no_hfignore: bool, timeout: int, **kwargs):
    """
    获取集群文件详情列表
    @param name:
    @param file_type:
    @param subpaths: 子目录列表
    @param no_checksum: 是否禁用checksum
    @return: list[FileInfo], int: FileInfo列表
    """
    file_list = FileList(files=subpaths)
    data = f'{{"file_list": {file_list.json()}}}'
    token = kwargs.get('token', mars_token())
    base_url = f'{mars_url()}/ugc/cloud/cluster_files/list?token={token}&name={name}&file_type={file_type}&no_checksum={no_checksum}&no_hfignore={no_hfignore}&recursive=True'

    ret: List[FileInfo] = []
    default_page_size = 100
    page = 1
    while True:
        url = f'{base_url}&page={page}&size={default_page_size}'
        result = await async_requests(RequestMethod.POST,
                                      url,
                                      retries=3,
                                      data=data,
                                      timeout=timeout)
        ret.extend([FileInfo(**d) for d in result['items']])
        if result['total'] > 10000:
            print('Tips: 集群侧文件数较多, 遍历耗时可能较长')
        if page * default_page_size >= result['total']:
            return ret
        page += 1


async def _do_poll_status(batch_size, completed_size, url, progress, task):
    while True:
        result = await async_requests(RequestMethod.GET,
                                      url,
                                      retries=5,
                                      timeout=10)
        if result['status'] == 'finished':
            return
        if result['status'] == 'failed':
            raise Exception(result['msg'])
        if result['status'] == 'running':
            current = int(result['msg'])
            if current > 0:
                # server端同步正在进行中
                progress.update(task, completed=current + completed_size)
            if current >= batch_size:
                return
        await sleep(4)


async def sync_to_cluster(name: str, file_type: str, files: List[FileInfo],
                          total_size: int, no_zip: bool, timeout: int,
                          **kwargs):
    """
    同步文件到集群
    @param name: 文件标识
    @param file_type: 文件类型
    @param files: 文件列表
    @param total_size: 总共上传的大小
    """
    token = kwargs.get('token', mars_token())
    completed_size = 0
    batch = 50

    with Progress() as progress:
        task = progress.add_task('syncing', total=total_size)
        for current_idx in range(0, len(files), batch):
            paths = [f.path for f in files[current_idx:current_idx + batch]]
            batch_size = sum([f.size for f in files[current_idx:current_idx + batch]])
            file_list = FileList(files=paths)
            url = f'{mars_url()}/ugc/sync_to_cluster?token={token}&name={name}&file_type={file_type}&no_zip={no_zip}'
            result = await async_requests(
                RequestMethod.POST,
                url,
                retries=3,
                timeout=timeout,
                data=f'{{"file_list": {file_list.json()}}}')
            index = result.get('index', None)
            if not index:
                index = hashkey(mars_token(), name, file_type, *paths)
            await _do_poll_status(
                batch_size, completed_size,
                f'{mars_url()}/ugc/sync_to_cluster/status?token={token}&index={index}',
                progress, task)
            completed_size += batch_size
            progress.update(task, completed=completed_size)


async def sync_from_cluster(name: str, file_type: str, files: List[FileInfo],
                            total_size: int, timeout: int, **kwargs):
    """
    从集群同步文件
    @param name: 文件标识
    @param file_type: 文件类型
    @param files: 文件列表
    @param total_size: 总共下载的大小
    """
    batch = 50
    token = kwargs.get('token', mars_token())
    completed_size = 0

    with Progress() as progress:
        task = progress.add_task('syncing', total=total_size)
        for current_idx in range(0, len(files), batch):
            file_infos = FileInfoList(files=files[current_idx:current_idx + batch])
            batch_size = sum([f.size for f in file_infos.files])
            url = f'{mars_url()}/ugc/sync_from_cluster?token={token}&name={name}&file_type={file_type}'
            result = await async_requests(
                RequestMethod.POST,
                url,
                retries=3,
                timeout=timeout,
                data=f'{{"file_infos": {file_infos.json()}}}')
            index = result.get('index', None)
            if not index:
                index = hashkey(mars_token(), name, file_type,
                                *[f.path for f in file_infos.files])
            await _do_poll_status(
                batch_size, completed_size,
                f'{mars_url()}/ugc/sync_from_cluster/status?token={token}&index={index}',
                progress, task)
            completed_size += batch_size
            progress.update(task, completed=completed_size)


############################################ 访问 cloud api ####################################################


async def get_cloud_api(provider, name, file_type, token_expires, connect_timeout=120, proxy=''):
    auth_token = await get_sts_token(provider, name, file_type, token_expires)
    kwargs = {
        'endpoint': auth_token['endpoint'],
        'access_key_id': auth_token['access_key_id'],
        'access_key_secret': auth_token['access_key_secret'],
        'security_token': auth_token['security_token'],
        'proxies': {'http': proxy, 'https': proxy} if proxy else None,
        'connect_timeout': connect_timeout,
    }
    if provider == 'oss':
        cloud_api = OSSApi(**kwargs)
    else:
        print_bold('Using mock cloud api, please check your provider config!')
        cloud_api = MockApi(**kwargs)
    return cloud_api, auth_token['bucket']


async def push_to_cluster(provider: str,
                          local_path: str,
                          remote_path: str,
                          name: str,
                          file_type: str,
                          force: bool = False,
                          no_checksum: bool = False,
                          no_hfignore: bool = False,
                          exclude_list: List = [],
                          no_zip: bool = False,
                          no_diff: bool = False,
                          list_timeout: int = 300,
                          sync_timeout: int = 300,
                          cloud_connect_timeout: int = 120,
                          token_expires: int = 1800,
                          part_mb_size: int = 100,
                          proxy: str = ''):
    """
    上传本地文件目录到云端 bucket，并删除远端孤儿目录，保持本地和远端目录一致
    @param local_path: 本地工作区目录
    @param remote_path: 远端工作区目录
    @param name: 标识
    @param file_type: 文件类型
    @param force: 是否强制推送并覆盖远端文件
    @param no_checksum: 是否禁用checksum
    @param no_hfignore: 是否禁用hfignore
    """
    local_only_files, cluster_only_files, changed_files, _ = await diff_local_cluster(
        local_path,
        name,
        file_type,
        subpath_list=['./'],
        no_checksum=no_checksum,
        no_hfignore=no_hfignore,
        no_diff=no_diff,
        exclude_list=exclude_list,
        timeout=list_timeout,
        is_push=True)
    # 校验是否有未下载数据
    if len(changed_files) != 0:  # 暂时忽略集群侧新文件 or len(cluster_only_files) != 0:
        print_diff(local_only_files,
                   cluster_only_files,
                   changed_files,
                   focus_list=['本地'])
        if not force:
            print('集群中存在差异文件，请确认，可增加 --force 参数强制覆盖!')
            return False
        print('集群中存在差异文件，将被本次操作覆盖!')
    target_files = local_only_files + changed_files
    if len(target_files) == 0:
        print('数据已同步，忽略本次操作')
        return True

    if not no_zip:
        print_bold(f'开始打包本地{file_type}目录...')
        zip_file_path = f'/tmp/{os.path.basename(local_path)}.zip'
        zip_dir(local_path, target_files, zip_file_path, exclude_list)
        target_files = [get_file_info(zip_file_path, '/tmp', no_checksum)]

    upload_size = 0
    for f in target_files:
        upload_size += f.size

    cloud_api, bucket_name = await get_cloud_api(provider, name, file_type, token_expires, cloud_connect_timeout, proxy)

    # 上传本地文件，注：需要在删除之后，避免误删
    # 小文件直接上传，大文件分片上传，阈值100MB
    await set_sync_status(file_type, name, SyncDirection.PUSH,
                          SyncStatus.STAGE1_RUNNING, local_path, remote_path)
    print_bold(f'(1/2) 开始同步本地目录 {local_path} 到远端，共{bytes_to_human(upload_size)}...')
    with Progress() as progress:
        task = progress.add_task('pushing', total=upload_size)
        completed_size = 0

        def percentage(consumed_bytes, total_bytes):
            if total_bytes:
                progress.update(task,
                                completed=consumed_bytes + completed_size)

        # 默认一天后过期
        expire_at = (datetime.utcnow().replace(
            tzinfo=timezone.utc).astimezone(tz_utc_8) +
                     timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
        part_size = part_mb_size * 1048576
        for f in target_files:
            src_file = f'{local_path}/{f.path}' if no_zip else f'/tmp/{f.path}'
            dst_file = f'{remote_path}/{f.path}'
            try:
                # 先校验云端是否有，避免打断情况下导致的重复上传，浪费带宽资源
                skip = False
                source = 'client'
                tagging = cloud_api.get_object_tagging(bucket_name, dst_file)
                if not no_checksum and 'md5' in tagging.keys():
                    skip = tagging.get('md5', '') == f.md5
                    source = tagging.get('source', 'client')
                if skip:
                    completed_size += f.size
                    progress.update(task, completed=completed_size)
                    continue
                src_file_mode = oct(stat.S_IMODE(os.lstat(src_file).st_mode))
                tagging = f'size={f.size}&source={source}&expire_at={expire_at}&filemode={src_file_mode}'
                if not no_checksum:
                    tagging += f'&md5={f.md5}'
                resumable_upload_with_retry(cloud_api,
                                            bucket_name,
                                            dst_file,
                                            src_file,
                                            multipart_threshold=part_size,
                                            part_size=part_size,
                                            percentage=percentage,
                                            num_threads=4,
                                            tagging=tagging)
                completed_size += f.size
            except Exception as e:
                print(f'上传 {f.path} 失败: {e}')
                await set_sync_status(file_type, name, SyncDirection.PUSH,
                                      SyncStatus.STAGE1_FAILED, local_path,
                                      remote_path)
                if not no_zip:
                    os.remove(f'/tmp/{os.path.basename(local_path)}.zip')
                return False
    await set_sync_status(file_type, name, SyncDirection.PUSH,
                          SyncStatus.STAGE1_FINISHED, local_path,
                          remote_path)

    if not no_zip:
        os.remove(f'/tmp/{os.path.basename(local_path)}.zip')

    print_bold('(2/2) 上传成功，开始同步到集群，请等待...')
    try:
        await sync_to_cluster(name, file_type, target_files, completed_size,
                              no_zip, sync_timeout)
    except Exception as e:
        print(str(e))
        return False
    return True


async def pull_from_cluster(provider: str,
                            local_path: str,
                            remote_path: str,
                            name: str,
                            file_type: str,
                            force: bool = False,
                            no_checksum: bool = False,
                            no_hfignore: bool = False,
                            subpath='./',
                            list_timeout: int = 300,
                            sync_timeout: int = 300,
                            cloud_connect_timeout: int = 120,
                            token_expires: int = 1800,
                            part_mb_size: int = 100,
                            proxy: str = ''):
    """
    下载bucket文件到本地
    @param local_path: 本地工作区目录
    @param remote_path: 远端工作区目录
    @param name:
    @param file_type:
    @param force: 是否强制推送并覆盖远端文件
    @param no_checksum: 是否禁用checksum
    @param no_hfignore: 是否禁用hfignore
    @param subpath: pull的子目录
    """
    local_only_files, cluster_only_files, _, changed_files = await diff_local_cluster(
        local_path, name, file_type, [subpath], no_checksum, no_hfignore, timeout=list_timeout)

    if len(changed_files) != 0:
        print_diff(local_only_files,
                   cluster_only_files,
                   changed_files,
                   focus_list=['集群'])
        if not force:
            print('本地存在差异文件，请确认，可增加 --force 参数强制覆盖!')
            return False
        print('本地存在差异文件，将被本次操作覆盖!')

    if len(cluster_only_files + changed_files) == 0:
        print('数据已同步，忽略本次操作')
        return True

    paths = []
    download_size = 0
    for f in cluster_only_files + changed_files:
        paths.append(f.path)
        download_size += f.size
    print_bold(f'(1/2) 开始同步集群数据，目录{paths}\n共{bytes_to_human(download_size)}, 请等待...')
    try:
        await sync_from_cluster(name, file_type,
                                cluster_only_files + changed_files,
                                download_size, sync_timeout)
    except Exception as e:
        print(str(e))
        return False

    print_bold(f'(2/2) 集群同步成功，开始下载远端目录到本地...')
    cloud_api, bucket_name = await get_cloud_api(provider, name, file_type, token_expires, cloud_connect_timeout, proxy)
    await set_sync_status(file_type, name, SyncDirection.PULL,
                          SyncStatus.STAGE2_RUNNING,
                          local_path, remote_path)
    with Progress() as progress:
        task = progress.add_task('pulling', total=download_size)
        completed_size = 0

        def percentage(consumed_bytes, total_bytes):
            if total_bytes:
                progress.update(task,
                                completed=consumed_bytes + completed_size)

        part_size = part_mb_size * 1048576
        for fi in cluster_only_files + changed_files:
            local = f'{local_path}/{fi.path}'
            remote = f'{remote_path}/{fi.path}'
            try:
                dirname = os.path.dirname(local)
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                resumable_download_with_retry(cloud_api,
                                              bucket_name,
                                              remote,
                                              local,
                                              multiget_threshold=part_size,
                                              part_size=part_size,
                                              percentage=percentage,
                                              num_threads=4)
                completed_size += fi.size

            except Exception as e:
                print(f'下载 {remote} 失败：{e}')
                await set_sync_status(file_type, name, SyncDirection.PULL,
                                      SyncStatus.STAGE2_FAILED,
                                      local_path, remote_path)
                return False
    await set_sync_status(file_type, name, SyncDirection.PULL,
                          SyncStatus.FINISHED, local_path, remote_path)
    return True


def resumable_download_with_retry(cloud_api, bucket_name, key, filename, multiget_threshold=None,
    part_size=None, percentage=None, num_threads=None, retries=3):
    download_succeed = False
    for i in range(1, retries + 1):
        try:
            if not download_succeed:
                cloud_api.resumable_download(bucket_name, key, filename, multipart_threshold=multiget_threshold,
                    part_size=part_size, percentage=percentage, num_threads=num_threads)
            download_succeed = True
            # 尝试从tag中恢复文件filemode
            try:
                tagging = cloud_api.get_object_tagging(bucket_name, key)
                filemode = tagging.get('filemode', None)
                if filemode:
                    os.chmod(filename, int(filemode, 8))
            except Exception as e:
                print(f'恢复文件{key}失败, 忽略: {str(e)}')
            return
        except Exception as e:
            if i == retries:
                raise
            else:
                print(f'第{i}次下载失败: {str(e)}, 尝试重试...')
                continue


def resumable_upload_with_retry(cloud_api, bucket_name, key, filename, multipart_threshold=None,
    part_size=None, percentage=None, num_threads=None, tagging=None, retries=3):
    for i in range(1, retries + 1):
        try:
            cloud_api.resumable_upload(bucket_name, key, filename,
                multipart_threshold=multipart_threshold, part_size=part_size,
                percentage=percentage, num_threads=num_threads, tagging=tagging)
            return
        except Exception as e:
            if i == retries:
                raise
            else:
                print(f'第{i}次上传失败: {str(e)}, 尝试重试...')
                continue
