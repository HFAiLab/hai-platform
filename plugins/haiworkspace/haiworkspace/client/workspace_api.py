
import munch
import sys

from .workspace_util import *

current_hf_run_path = os.getcwd()
workspace_config_file = './.hfai/workspace.yml'


def update_current_hf_run_path(new_path):
    global current_hf_run_path
    current_hf_run_path = new_path


async def init(workspace=None, provider=None):
    """
    这里的设计思路，会和 git 一样
        1. python xxx.py 的时候，会一层一层找到 workspace
        2. 如果有两个同名的 workspace，会覆盖，这边我们是不处理这个逻辑的
        3. 用户可以把本地多个文件夹都作为一个 workspace
    @param str workspace: 本地工作区路径
    @return:
    """
    for i in ['.', '..', '/', '\\', '~']:
        if i in workspace:
            print('workspace命名请不要包含特殊字符!')
            sys.exit(1)
    if current_hf_run_path in ['/', '/usr', '/sys', '/proc', '/boot', '/dev', '/run']:
        print('无法创建系统目录为workspace, 请切换当前目录')
        sys.exit(1)

    os.makedirs(os.path.join(current_hf_run_path, './.hfai'), exist_ok=True)
    wcf = os.path.join(current_hf_run_path, workspace_config_file)
    if os.path.exists(wcf):
        wc = munch.Munch.fromYAML(open(wcf))
        if wc.workspace == workspace:
            print('已经创建了这个 workspace')
        else:
            print(f'当前目录已经配置为 workspace: {wc.workspace}，请确认')
        return

    print('查询用户 group 信息...')
    user_info = await async_requests(RequestMethod.POST, f'{mars_url()}/query/user/info?token={mars_token()}')
    user_info = user_info['result']
    cwd = current_hf_run_path
    # 转换windows路径格式为unix格式
    drive, cwd = os.path.splitdrive(cwd)
    if drive != '':
        cwd = cwd.replace('\\', '/')

    if not workspace:
        workspace = os.path.basename(cwd)
    exclude_path = ['/ceph-jd', '/weka-jd', '/opt/hf_venvs', '/hf_shared']
    if any(cwd.startswith(p) for p in exclude_path):
        print('检测到是在萤火集群的代码, 忽略本次操作，请在集群外使用 workspace 功能')
        sys.exit(1)

    remote_path = f'{user_info["user_shared_group"]}/{user_info["user_name"]}/workspaces/{workspace}'
    workspace_config = munch.Munch.fromDict({
        "workspace": workspace,
        'local': cwd,
        'remote': remote_path,
        'provider': provider,
    })

    await set_sync_status(FileType.WORKSPACE, workspace, SyncDirection.PUSH, SyncStatus.INIT, cwd, remote_path)
    with open(os.path.join(current_hf_run_path, workspace_config_file),
              encoding='utf8', mode='w') as f:
        f.write(workspace_config.toYAML())
    print(
        f'初始化 workspace [{workspace_config.local}]->[{provider}://{workspace_config.remote}] 成功')


def get_workspace_conf():
    """
    获取这个目录的 workspace 配置文件，返回 workspace config 的文件
    @return: workspace_config_file
    """
    cwd = current_hf_run_path
    subs = []
    while cwd != '/':
        wcf = os.path.join(cwd, workspace_config_file)
        if os.path.exists(wcf):
            return wcf, subs
        subs.insert(0, os.path.basename(cwd))
        cwd = os.path.dirname(cwd)
    return None, None


async def get_wc_with_check():
    wcf, _ = get_workspace_conf()
    if not wcf:
        print('workspace未初始化')
        return None
    wc = munch.Munch.fromYAML(open(wcf))
    try:
        _ = wc.provider
    except:
        wc.provider = 'oss'
    result = await get_sync_status(FileType.WORKSPACE, wc.workspace)
    if len(result) == 0:
        print(f"没找到工作区 {wc.workspace}")
        return None
    return wc


async def push(force: bool = False, no_checksum: bool = False, no_hfignore: bool = False, no_zip: bool = False, no_diff: bool = False,
    list_timeout: int = 300, sync_timeout: int = 300, cloud_connect_timeout: int = 120, token_expires: int = 1800, part_mb_size: int = 100,
    proxy: str = '', file_type: str = FileType.WORKSPACE, env_provider: str = 'oss', env_local_path: str = '', env_remote_path: str = ''):
    """
    推送本地workspace到集群
    @param force: 是否强制推送
    @param no_checksum: 是否禁用checksum
    @param no_hfignore: 是否禁用hfignore
    @return bool: 标识push是否成功
    """
    # 一层一层往上面找，看看有没有 ./hfai/workspace.yml
    if file_type == FileType.WORKSPACE:
        wc = await get_wc_with_check()
        if wc is None:
            return False
        provider, local_path, remote_path, name = wc.provider, wc.local, wc.remote, wc.workspace
        exclude_list = []
    elif file_type == FileType.ENV:
        provider, local_path, remote_path, name = env_provider, env_local_path, env_remote_path, os.path.basename(env_remote_path)
        exclude_list = ['activate', 'pip.conf']
    else:
        print(f'不支持的file_type: {file_type}')
        return False
    kwargs = {
        'provider': provider,
        'local_path': local_path,
        'remote_path': remote_path,
        'name': name,
        'file_type': file_type,
        'force': force,
        'no_checksum': no_checksum,
        'no_hfignore': no_hfignore,
        'exclude_list': exclude_list,
        'no_zip': no_zip,
        'no_diff': no_diff,
        'list_timeout': list_timeout,
        'sync_timeout': sync_timeout,
        'cloud_connect_timeout': cloud_connect_timeout,
        'token_expires': token_expires,
        'part_mb_size': part_mb_size,
        'proxy': proxy
    }

    return await push_to_cluster(**kwargs)


async def pull(force: bool = False, no_checksum: bool = False, no_hfignore: bool = False, subpath: str = '', list_timeout: int = 300,
    sync_timeout: int = 300, cloud_connect_timeout: int = 120, token_expires: int = 1800, part_mb_size: int = 100, proxy: str = ''):
    """
    从集群下载workspace数据到本地
    @param force: 是否覆盖本地文件
    @param no_checksum: 是否禁用checksum
    @param no_hfignore: 是否禁用hfignore
    @param subpath: pull的子目录
    @return bool: 标识pull是否成功
    """
    # 一层一层往上面找，看看有没有 ./hfai/workspace.yml
    wc = await get_wc_with_check()
    if wc is None:
        return False
    while subpath.startswith('.' + os.path.sep):  # 为了在ignore的时候不出问题
        subpath = subpath[len('.' + os.path.sep):]
    return await pull_from_cluster(wc.provider, wc.local, wc.remote, wc.workspace, FileType.WORKSPACE, force=force,
        no_checksum=no_checksum, no_hfignore=no_hfignore, subpath=subpath, list_timeout=list_timeout,
        cloud_connect_timeout=cloud_connect_timeout, sync_timeout=sync_timeout, token_expires=token_expires, part_mb_size=part_mb_size, proxy=proxy)


async def diff(no_checksum: bool = False, no_hfignore: bool = False, list_timeout: int = 3600):
    """
    比较本地和集群目录的diff
    @param no_checksum: 是否禁用checksum
    @param no_hfignore: 是否禁用hfignore
    """
    wc = await get_wc_with_check()
    if wc is None:
        return False
    local_only_files, cluster_only_files, _, changed_files = await diff_local_cluster(wc.local, wc.workspace,
        FileType.WORKSPACE, ['./'], no_checksum=no_checksum, no_hfignore=no_hfignore, timeout=list_timeout)
    print_diff(local_only_files, cluster_only_files, changed_files)


async def list():
    current_ws = ''
    wcf, _ = get_workspace_conf()
    if wcf:
        wc = munch.Munch.fromYAML(open(wcf))
        current_ws = wc.workspace

    result = await get_sync_status(FileType.WORKSPACE)
    console = Console()
    table = Table(box=ASCII2, style='dim', show_header=True)
    for column in ['workspace', 'local_path', 'cluster_path', 'push status', 'last push', 'pull status', 'last pull']:
        table.add_column(column)
    for f in result:
        table.add_row(f['name'], f['local_path'], f['cluster_path'], f['push_status'], f['last_push'], f['pull_status'], f['last_pull'], 
            style=('bold' if f['name'] == current_ws else None), end_section=True)
    console.print(table)


async def delete(workspace_name, files):
    for f in files:
        if '..' in f:
            raise Exception(f'非法文件路径 "{f}"')
    current_ws = ''
    wcf, _ = get_workspace_conf()
    if wcf:
        wc = munch.Munch.fromYAML(open(wcf))
        current_ws = wc.workspace

    result = await get_sync_status(FileType.WORKSPACE, workspace_name)
    if len(result) == 0:
        print(f"没找到工作区 {workspace_name}")
        return
    await delete_workspace(workspace_name, files)
    if len(files) == 0:
        if current_ws == workspace_name:
            os.remove(wcf)
        else:
            print(f'请手动删除本地工作区{workspace_name}目录下 .hfai/workspace.yml 文件！')
