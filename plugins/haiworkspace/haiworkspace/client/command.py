import asyncclick as click
import sys
from . import workspace_api


class HandleHaiWorkspaceCommandArgs(click.Command):
    def format_usage(self, ctx, formatter):
        pieces = self.collect_usage_pieces(ctx)
        pieces = [f'<{p}>' for p in pieces[1:]] + ['[OPTIONS]']
        formatter.write_usage(ctx.command_path, " ".join(pieces))

    def format_options(self, ctx, formatter):
        pieces = self.collect_usage_pieces(ctx)
        with formatter.section("Arguments"):
            if 'workspace_name' in pieces:
                formatter.write_dl(rows=[('workspace_name', '工作区的名字')])
            if 'remote_path' in pieces:
                formatter.write_dl(rows=[('remote_path', '远端文件路径，远端目录请通过diff获取，如 checkpoint/model.pt，默认: checkpoint')])
        super(HandleHaiWorkspaceCommandArgs, self).format_options(ctx, formatter)


@click.command(cls=HandleHaiWorkspaceCommandArgs)
@click.argument('workspace_name', required=True, metavar='workspace_name')
@click.option('-p', '--provider', required=False, is_flag=False, default='oss', show_default=True, help='使用的云端存储服务类别')
async def init(workspace_name, provider):
    """
    初始化本地工作区
    """
    try:
        await workspace_api.init(workspace_name, provider)
    except Exception as e:
        print(f'初始化失败，错误信息：{e}')
        sys.exit(1)


@click.command(cls=HandleHaiWorkspaceCommandArgs)
@click.option('--force', required=False, is_flag=True, default=False, help='是否强制推送并覆盖远端目录, 默认值为False')
@click.option('-n', '--no_checksum', required=False, is_flag=True, default=False, help='是否对文件禁用checksum比对, 默认值为False')
@click.option('-i', '--no_hfignore', required=False, is_flag=True, default=False, help='是否忽略.hfignore规则，默认值为False')
@click.option('-z', '--no_zip', required=False, is_flag=True, default=False, help='是否禁用workspace打包上传, 默认值为False')
@click.option('-d', '--no_diff', required=False, is_flag=True, default=False, help='是否禁用差量上传, 如是, 本地和远端不一致文件将被强制覆盖, 默认值为False')
@click.option('-l', '--list_timeout', required=False, is_flag=False, type=click.IntRange(5, 7200), default=300, show_default=True, help='遍历集群工作区的超时时间, 单位(s)')
@click.option('-s', '--sync_timeout', required=False, is_flag=False, type=click.IntRange(5, 21600), default=1800, show_default=True, help='等待同步任务提交成功的超时时间, 单位(s)')
@click.option('-o', '--cloud_connect_timeout', required=False, is_flag=False, type=click.IntRange(60, 43200), default=120, show_default=True, help='从本地上传分片到云端的连接超时时间, 单位(s)')
@click.option('-t', '--token_expires', required=False, is_flag=False, type=click.IntRange(900, 43200), default=1800, show_default=True, help='从本地上传到云端的sts token有效时间, 单位(s)')
@click.option('-p', '--part_mb_size', required=False, is_flag=False, type=click.IntRange(10, 10240), default=100, show_default=True, help='从本地上传到云端的分片大小, 单位(MB)')
@click.option('--proxy', required=False, is_flag=False, default='', help='从本地上传到云端时使用的代理url')
@click.option('--file_type', required=False, is_flag=False, default='workspace', hidden=True, show_default=True, help='env特定选项: 文件类型 workspace/env')
@click.option('--env_provider', required=False, is_flag=False, default='oss', hidden=True, show_default=True, help='env特定选项: 使用的云端存储服务类别')
@click.option('--env_local_path', required=False, is_flag=False, default='', hidden=True, show_default=True, help='env特定选项: 本地路径')
@click.option('--env_remote_path', required=False, is_flag=False, default='', hidden=True, show_default=True, help='env特定选项: 集群路径')
async def push(force, no_checksum, no_hfignore, no_zip, no_diff, list_timeout, sync_timeout,
    cloud_connect_timeout, token_expires, part_mb_size, proxy, file_type, env_provider, env_local_path, env_remote_path):
    """
    推送本地workspace到萤火二号
    """
    try:
        pushed = await workspace_api.push(force=force, no_checksum=no_checksum, no_hfignore=no_hfignore, no_zip=no_zip, no_diff=no_diff,
            list_timeout=list_timeout, sync_timeout=sync_timeout, cloud_connect_timeout=cloud_connect_timeout, token_expires=token_expires,
            part_mb_size=part_mb_size, proxy=proxy, file_type=file_type, env_provider=env_provider,
            env_local_path=env_local_path, env_remote_path=env_remote_path)
        if not pushed:
            print('推送失败，请稍后重试，或者联系管理员...')
            sys.exit(1)
        else:
            print('推送成功')
    except Exception as e:
        print(f'推送失败，错误信息：{e}')
        sys.exit(1)


@click.command(cls=HandleHaiWorkspaceCommandArgs)
@click.option('--force', required=False, is_flag=True, default=False, help='是否强制覆盖本地目录，默认值为False')
@click.option('-n', '--no_checksum', required=False, is_flag=True, default=False, help='是否对文件禁用checksum比对，默认值为False')
@click.option('-i', '--no_hfignore', required=False, is_flag=True, default=False, help='是否忽略.hfignore规则，默认值为False')
@click.option('-l', '--list_timeout', required=False, is_flag=False, type=click.IntRange(5, 7200), default=300, show_default=True, help='遍历集群工作区的超时时间, 单位(s)')
@click.option('-s', '--sync_timeout', required=False, is_flag=False, type=click.IntRange(5, 21600), default=1800, show_default=True, help='等待同步任务提交成功的超时时间, 单位(s)')
@click.option('-o', '--cloud_connect_timeout', required=False, is_flag=False, type=click.IntRange(60, 43200), default=120, show_default=True, help='从云端下载分片到本地的连接超时时间, 单位(s)')
@click.option('-t', '--token_expires', required=False, is_flag=False, type=click.IntRange(900, 43200), default=1800, show_default=True, help='从云端下载到本地的sts token有效时间, 单位(s)')
@click.option('-p', '--part_mb_size', required=False, is_flag=False, type=click.IntRange(10, 10240), default=100, show_default=True, help='从云端下载到本地的分片大小, 单位(MB)')
@click.option('--proxy', required=False, is_flag=False, default='', help='从云端下载到本地时使用的代理url')
async def pull(force, no_checksum, no_hfignore, list_timeout, sync_timeout, cloud_connect_timeout, token_expires, part_mb_size, proxy):
    """
    下载萤火二号目录到本地workspace
    """
    try:
        pulled = await workspace_api.pull(force=force, no_checksum=no_checksum, no_hfignore=no_hfignore, list_timeout=list_timeout, sync_timeout=sync_timeout,
            cloud_connect_timeout=cloud_connect_timeout, token_expires=token_expires, part_mb_size=part_mb_size, proxy=proxy)
        if not pulled:
            print('下载失败，请稍后重试，或者联系管理员...')
            sys.exit(1)
        else:
            print('下载成功')
    except Exception as e:
        print(f'下载失败，错误信息：{e}')
        sys.exit(1)


@click.command(cls=HandleHaiWorkspaceCommandArgs)
@click.argument('remote_path', required=True, default='checkpoint', metavar='remote_path')
@click.option('--force', required=False, is_flag=True, default=False, help='是否强制覆盖本地目录，默认值为False')
@click.option('-n', '--no_checksum', required=False, is_flag=True, default=False, help='是否对文件禁用checksum比对，默认值为False')
@click.option('-i', '--no_hfignore', required=False, is_flag=True, default=False, help='是否忽略.hfignore规则，默认值为False')
@click.option('-l', '--list_timeout', required=False, is_flag=False, type=click.IntRange(5, 7200), default=300, show_default=True, help='遍历集群工作区的超时时间，单位(s)')
@click.option('-s', '--sync_timeout', required=False, is_flag=False, type=click.IntRange(5, 21600), default=1800, show_default=True, help='等待同步任务提交成功的超时时间, 单位(s)')
@click.option('-o', '--cloud_connect_timeout', required=False, is_flag=False, type=click.IntRange(60, 43200), default=120, show_default=True, help='从云端下载分片到本地的连接超时时间, 单位(s)')
@click.option('-t', '--token_expires', required=False, is_flag=False, type=click.IntRange(900, 43200), default=1800, show_default=True, help='从云端下载到本地的sts token有效时间, 单位(s)')
@click.option('-p', '--part_mb_size', required=False, is_flag=False, type=click.IntRange(10, 10240), default=100, show_default=True, help='从云端下载到本地的分片大小, 单位(MB)')
@click.option('--proxy', required=False, is_flag=False, default='', help='从云端下载到本地时使用的代理url')
async def download(remote_path, force, no_checksum, no_hfignore, list_timeout, sync_timeout, cloud_connect_timeout, token_expires, part_mb_size, proxy):
    """
    下载萤火二号目录中指定文件到本地，
    远端目录请通过diff获取，如 checkpoint/model.pt
    """
    if remote_path == '':
        print('请指定远端文件路径')
        sys.exit(1)
    try:
        pulled = await workspace_api.pull(force=force, no_checksum=no_checksum, no_hfignore=no_hfignore, subpath=remote_path, list_timeout=list_timeout, sync_timeout=sync_timeout,
            cloud_connect_timeout=cloud_connect_timeout, token_expires=token_expires, part_mb_size=part_mb_size, proxy=proxy)
        if not pulled:
            print('下载失败，请稍后重试，或者联系管理员...')
            sys.exit(1)
        else:
            print('下载成功')
    except Exception as e:
        print(f'下载失败，错误信息：{e}')
        sys.exit(1)


@click.command(cls=HandleHaiWorkspaceCommandArgs)
@click.option('-n', '--no_checksum', required=False, is_flag=True, default=False, help='是否对文件禁用checksum比对，默认值为False')
@click.option('-i', '--no_hfignore', required=False, is_flag=True, default=False, help='是否忽略.hfignore规则，默认值为False')
@click.option('-l', '--list_timeout', required=False, is_flag=False, type=click.IntRange(5, 7200), default=300, show_default=True, help='遍历集群工作区的超时时间，单位(s)')
async def diff(no_checksum, no_hfignore, list_timeout):
    """
    比较本地workspace和萤火二号目录diff，默认比较文件md5\n
    如耗时较长，可通过'--no_checksum'参数禁用md5计算，只通过文件size比较，该方法不可靠
    """
    try:
        await workspace_api.diff(no_checksum, no_hfignore, list_timeout)
    except Exception as e:
        print(f'diff失败，错误信息：{e}')
        sys.exit(1)


@click.command(cls=HandleHaiWorkspaceCommandArgs)
async def list():
    """
    列举所有萤火二号workspace，加粗表示为当前运行目录所在workspace
    """
    try:
        await workspace_api.list()
    except Exception as e:
        print(f'list失败，错误信息：{e}')
        sys.exit(1)


@click.command(cls=HandleHaiWorkspaceCommandArgs)
@click.argument('workspace_name', required=True, metavar='workspace_name')
@click.option('-f', '--files', required=False, default=(), multiple=True, help='保留工作区，仅删除指定的集群侧文件/文件目录列表，不支持通配符; 如不指定则为删除整个工作区')
@click.confirmation_option(prompt='请确认是否删除 workspace, 此操作将删除集群侧工作目录且无法复原！')
async def remove(workspace_name, files):
    """
    删除集群工作区

    示例: "hfai workspace remove demo -f checkpoint/ -f output/train.log"
    """
    if not workspace_name:
        print("请输入工作区名字")
        sys.exit(1)
    try:
        await workspace_api.delete(workspace_name, files)
        print(f'remove 完成')
    except Exception as e:
        print(f'remove 失败，错误信息：{e}')
        sys.exit(1)
