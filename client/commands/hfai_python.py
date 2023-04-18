import os
import posixpath
import sys
import shlex
from io import StringIO
import yaml

import asyncclick as click
import munch
from rich.console import Console

import hfai.client.commands.hfai_experiment as hfai_experiment
from .utils import is_hai, CLI_NAME, get_workspace_conf

cmd = None
file_ext = None
if len(sys.argv) >= 2:
    cmd = sys.argv[1]  # bash or python or ext
    if cmd == 'bash':
        file_ext = 'sh'
    elif cmd == 'python':
        file_ext = 'py'
    elif cmd == 'exec':
        file_ext = 'exe'


class JobType:
    CLUSTER = '1'
    SIMULATE = '2'
    LOCAL = '3'


EXPIRED_ARGS = ['--detach']


class HandleHfaiPythonArgs(click.Command):
    def format_usage(self, ctx, formatter):
        if os.environ.get('external') == 'true':
            formatter.write_usage(f'''
    {CLI_NAME} {cmd} <experiment.{file_ext}> [<experiment_params>...]                        # 在本地运行，等同 {cmd}
    {CLI_NAME} {cmd} <experiment.{file_ext}> [<experiment_params>...] -- [CLUSTER_OPTIONS]   # 提交任务到萤火二号运行
    {CLI_NAME} {cmd} <experiment.{file_ext}> [<experiment_params>...] ++ [SIMULATE_OPTIONS]  # 在本地模拟运行，会提供萤火二号一致的环境变量和表现''')
        else:
            formatter.write_usage(f'''
    {CLI_NAME} {cmd} <experiment.{file_ext}> [<experiment_params>...] -- [CLUSTER_OPTIONS]   # 提交任务到萤火二号运行''')

    def format_help(self, ctx, formatter):
        super(HandleHfaiPythonArgs, self).format_help(ctx, formatter)
        with formatter.section("Examples"):
            formatter.write_text(f'{CLI_NAME} {cmd} bert.{file_ext} -c large.yml -- -n 10 # 使用萤火十节点')
            if not is_hai:
                formatter.write_text(f'HF_ENV_NAME=202111 {CLI_NAME} {cmd} bert.{file_ext} -c large.yml -- -n 1 # 使用 hf_env38 202111 运行')
            if os.environ.get('external') == 'true':
                formatter.write_text(f'{CLI_NAME} {cmd} bert.{file_ext} -c large.yml  # 本地运行')
                formatter.write_text(f'{CLI_NAME} {cmd} bert.{file_ext} -c large.yml -- --nodes 1  # 使用萤火一节点')
                formatter.write_text(f'{CLI_NAME} {cmd} bert.{file_ext} -c large.yml -- -n 1 -i default -e A=B -e C=D')
                formatter.write_text(f'{CLI_NAME} {cmd} bert.{file_ext} -c large.yml -- -n 1')
                formatter.write_text(f'{CLI_NAME} {cmd} bert.{file_ext} -c large.yml ++ --suspend_seconds 100 # 本地模拟100秒后打断')
                formatter.write_text(f'{CLI_NAME} {cmd} bert.{file_ext} -c large.yml ++ -ss 100 -ls 1')
            else:  # internal
                formatter.write_text(f'{CLI_NAME} {cmd} bert.{file_ext} -c large.yml -- --nodes 1 --group jd_a100 --priority 40')
                formatter.write_text(f'{CLI_NAME} {cmd} bert.{file_ext} -c large.yml -- -n 1 -i default -e A=B -e C=D')
                formatter.write_text(f'{CLI_NAME} {cmd} bert.{file_ext} -c large.yml -- -n 1 -g jd_a100 -p 40')

    def format_options(self, ctx, formatter):
        with formatter.section("Arguments"):
            words = '可执行文件' if cmd == 'exec' else '脚本'
            formatter.write_dl(rows=[(f'experiment.{file_ext}', f'远程运行的{words}')])
            formatter.write_dl(rows=[(f'experiment_params...', f'远程运行的{words}需要的一系列参数，可输入多项，与本地运行的时候一致')])

        cluster_opts = []
        simulate_opts = []
        workspace_opts = []
        opts = []
        for param in self.get_params(ctx):
            rv = param.get_help_record(ctx)
            if rv is not None:
                if any([n in param.name for n in ['suspend_seconds', 'life_state']]):
                    simulate_opts.append(rv)
                elif 'help' in param.name:
                    opts.append(rv)
                elif rv[-1].startswith('push时'):
                    workspace_opts.append(rv)
                else:
                    if os.environ.get('external') == 'true' and any([n in param.name for n in ['group', 'priority']]):
                        continue
                    cluster_opts.append(rv)

        with formatter.section("Cluster Options"):
            formatter.write_dl(cluster_opts)

        if not is_hai:
            if os.environ.get('external') == 'true':
                with formatter.section("Simulate Options"):
                    formatter.write_dl(simulate_opts)

            with formatter.section("Extra Cluster Options (--options)"):
                formatter.write_dl(rows=[(f'py_venv=<var>', '指定在萤火上以什么 hf_env 运行, 可选萤火内建的：202111, 或使用自己的 hfai_env')])
                formatter.write_dl(rows=[(f'profile.(time | recorder)=<var>', '指定 haiprof 的配置。time 代表运行的时间（s）；recorder 代表需要采集的指标，用逗号分隔，比如 all 或者 basic,gpu')])

            with formatter.section("Environment Variables"):
                formatter.write_dl(rows=[(f'HF_ENV_NAME=<var>', '用于显示指定在萤火上以什么 hf_env 运行, 可选萤火内建的：202111, 或使用自己的 hfai_env')])
                formatter.write_dl(rows=[(f'HF_ENV_OWNER=<var>', f'若使用他人的 hfai_env，需要指定是谁的，可以通过 {CLI_NAME} venv list 查看')])

            if os.environ.get('external') == 'true':  # hai 不开放 workspace
                with formatter.section("Workspace Options"):
                    formatter.write_dl(workspace_opts)

    async def parse_args(self, ctx, args):
        assert not ('--' in args and '++' in args), '集群运行参数[--] 与模拟运行参数[++] 不能并存'
        assert len([arg for arg in args if arg == '--']) <= 1, '集群运行参数[--] 不能重复填写'
        assert len([arg for arg in args if arg == '++']) <= 1, '集群运行参数[++] 不能重复填写'
        if '--' in args:
            job_type = JobType.CLUSTER
        elif '++' in args:
            job_type = JobType.SIMULATE
        else:
            job_type = JobType.LOCAL

        if len(args) > 0:
            if job_type is not JobType.LOCAL:
                flag = '--' if job_type == JobType.CLUSTER else '++'
                _idx = args.index(flag)
                if len(args) >= 1:
                    args = [job_type, args[0]] + [tuple(args[1: _idx])] + args[_idx+1: ]
            else:
                if len(args) > 1:
                    args = [job_type, args[0]] + [tuple(args[1: ])]
                else:
                    args = [job_type, args[0]] + [tuple()]
        else:
            args = ['python']
        args = [a for a in args if a not in EXPIRED_ARGS]
        return await super(HandleHfaiPythonArgs, self).parse_args(ctx, args)


def func_python_local(experiment_py, experiment_args, suspend_seconds, life_state):
    console = Console()
    # 以下几个是和训练相关的的 env
    envs = ['HFAI_SIMULATE=1', 'WORLD_SIZE=1', 'RANK=0', 'MASTER_IP=127.0.0.1',
            'MASTER_PORT=29510', f'MARSV2_WHOLE_LIFE_STATE={life_state or 0}']
    console.print('初始化模拟环境 hfai environ, 请在代码中使用下面环境变量:', ','.join(envs))
    if suspend_seconds is not None:
        envs.append(f'SIMULATE_SUSPEND={suspend_seconds}')
        console.print(f'设置了 模拟打断时间 ，训练将在 {suspend_seconds} 秒之后打断')
    experiment_args = eval(experiment_args) if type(experiment_args) is str else experiment_args  # note asyncclick 8 tuple will be str
    experiment_args = [shlex.quote(arg) for arg in experiment_args]  # py3.8 we have shlex join
    code_cmd = ' '.join(envs) + f' {cmd} ' + experiment_py + ' ' + ' '.join(experiment_args)
    console.print(f'{CLI_NAME} {cmd} 模拟运行 {code_cmd}')
    os.system(code_cmd)


async def func_python_cluster(experiment_py: str, experiment_args, name, nodes, priority, group, image, environments, no_inherit, follow,
    force, no_checksum, no_hfignore, no_zip, no_diff, list_timeout, sync_timeout, cloud_connect_timeout, token_expires, part_mb_size, proxy, options):
    experiment_args = eval(experiment_args) if type(experiment_args) is str else experiment_args
    # note: 为了处理 python a.py --test="(a > b)" 的情况
    #              将上者转换成 python a.py '--test=(a > b)'，应该是等价的
    experiment_args = [shlex.quote(arg) for arg in experiment_args]  # py3.8 we have shlex join
    parameters = " ".join(experiment_args)

    workspace_dir = os.getcwd()
    if os.path.isabs(experiment_py):
        new_workspace_dir = os.path.dirname(experiment_py)
        workspace_dir = new_workspace_dir
        experiment_py = os.path.basename(experiment_py)

    assert ' ' not in workspace_dir, '不支持带空格的目录'
    # 如果是 workspace 代码，那么先上传，同时替换掉 directory 和 entrypoint
    # quick fix，如果是在 ceph 上，那么直接是 workspace
    wcf, subs = get_workspace_conf(workspace_dir)
    if os.environ.get('external') == 'true':
        if not wcf:
            # 不默认初始化在共享盘上的
            mount_list = [p for p in os.environ.get('MOUNT_LIST', '').split(',') if p != '']
            # mount_list += ['/ceph-jd', '/weka-jd', '/opt/hf_venvs']
            if any(workspace_dir.startswith(p) for p in mount_list):
                print('检测到是在萤火集群的代码，就不主动初始化成环境了')
            else:
                print('workspace 尚未初始化')
                sys.exit(1)
        else:
            wc = munch.Munch.fromYAML(open(wcf))
            print(f'检测到是 [{wc.local}] -> [{wc.remote}] 中的代码，先推送到远端...')
            workspace_cmd = f"{sys.argv[0]} workspace push --list_timeout {list_timeout} --sync_timeout {sync_timeout} --cloud_connect_timeout {cloud_connect_timeout} --token_expires {token_expires} --part_mb_size {part_mb_size}"
            if force:
                workspace_cmd += ' --force'
            if no_checksum:
                workspace_cmd += ' --no_checksum'
            if no_hfignore:
                workspace_cmd += ' --no_hfignore'
            if no_zip:
                workspace_cmd += ' --no_zip'
            if no_diff:
                workspace_cmd += ' --no_diff'
            if proxy:
                workspace_cmd += f' --proxy {proxy}'
            if os.system(workspace_cmd):
                sys.exit(1)
            try:
                _ = wc.provider
            except:
                wc.provider = 'oss'
            workspace_dir = posixpath.join(f'{wc.provider}://', wc.remote)
            experiment_py = posixpath.join(*subs, experiment_py)
    hf_env_name, hf_env_owner = ('', '') if no_inherit else (os.environ.get('HF_ENV_NAME', ''), os.environ.get('HF_ENV_OWNER', ''))
    if not no_inherit:
        print('\033[1;35m WARNING: \033[0m', f'提交的任务将会继承当前环境\033[1;33m {hf_env_name}\033[0m，有可能造成环境不兼容，如不想继承当前环境请添加参数\033[1;34m --no_inherit \033[0m')
    experiment_yaml = f'''
version: 2
name: {name or experiment_py}
priority: {priority}
spec:
  workspace: {workspace_dir}
  entrypoint: {experiment_py}
resource:
  image: {image}
  group: {group}
  node_count: {nodes}
    '''
    # note: params 直接变成 string 会有 parse 问题
    experiment_yaml = os.path.expanduser(experiment_yaml)
    config: munch.Munch = munch.Munch.fromYAML(StringIO(experiment_yaml))
    config.spec.parameters = parameters  # 在这里设置 params
    # env 也是一样
    envs = {}
    if environments:
        for e in environments:
            idx = e.index('=')
            key = e[0:idx]
            value = e[idx + 1:]
            envs[key] = shlex.quote(value)
        config.spec.environments = envs
    if cmd == 'exec':
        config.spec.entrypoint_executable = True
    if hf_env_name != '':
        config.options = {'py_venv': f'{hf_env_name}' + (f'[{hf_env_owner}]' if hf_env_owner else '')}

    if options:
        options_dict = config.get('options', {})
        for kv in options:
            node = options_dict
            key, value = kv.strip().split('=')
            for node_name in key.split('.')[:-1]:
                node[node_name] = node.get(node_name, {})
                node = node[node_name]

            leaf = key.split('.')[-1]
            node[leaf] = yaml.safe_load(value)
        config.options = options_dict

    print('提交任务成功，定义如下')
    print('-' * 80)
    print(yaml.dump(config.toDict()))
    print('-' * 80)
    await hfai_experiment.run.callback(config, follow, None, None, None)


@click.command(cls=HandleHfaiPythonArgs, context_settings=dict(ignore_unknown_options=True))
@click.argument('experiment_type')
@click.argument('experiment_py', metavar=f'experiment.{file_ext}')
@click.argument('experiment_args')
@click.option('--name', type=str, help='指定任务的名字，不指定的话，不填默认为文件名', default='')
@click.option('-n', '--nodes', type=int, help='用多少个节点跑，必填')
@click.option('-g', '--group', help='任务跑在哪个分组，不填默认为GPU分组', default='default')
@click.option('-p', '--priority', type=int, help='任务优先级，内部用户选填从低到高: 20, 30, 40, 50， 外部用户不用填，使用默认优先级', default=30)
@click.option('-i', '--image', type=str, default=os.environ.get('MARSV2_TASK_BACKEND', 'default'), help='使用哪个镜像跑任务, 默认采用当前镜像，否则内部用户默认cuda_11，外部用户默认 ubuntu2024-cu113-ext, 也可以通过 hfai images 自定义镜像')
@click.option('-e', '--environments', type=str, multiple=True, help='任务运行中需要的环境变量，举例，-e A=X -e B=Y; 则定义了 A, B 两个环境变量')
@click.option('-f', '--follow', required=False, is_flag=True, default=False, show_default=True, help='是否追加查看日志')
@click.option('--no_inherit', required=False, is_flag=True, default=False, show_default=True, help='上传到集群的任务是否使用当前的hf_env环境')
@click.option('-ss', '--suspend_seconds', type=int, help='模拟任务在多少秒时候打断')
@click.option('-ls', '--life_state', type=int, help='传入任务的 whole_life_state flag 值', default=0)
@click.option('--force', required=False, is_flag=True, default=False, show_default=True, help='push时, 是否强制推送并覆盖远端目录')
@click.option('--no_checksum', required=False, is_flag=True, default=False, show_default=True, help='push时, 是否对文件禁用checksum')
@click.option('--no_hfignore', required=False, is_flag=True, default=False, show_default=True, help='push时, 是否忽略.hfignore规则')
@click.option('--no_zip', required=False, is_flag=True, default=False, help='push时, 是否禁用workspace打包上传')
@click.option('--no_diff', required=False, is_flag=True, default=False, help='push时, 是否禁用差量上传, 如是, 本地和远端不一致文件将被强制覆盖, 默认值为False')
@click.option('--list_timeout', required=False, is_flag=False, type=click.IntRange(5, 7200), default=300, show_default=True, help='push时, 遍历集群工作区的超时时间, 单位(s)')
@click.option('--sync_timeout', required=False, is_flag=False, type=click.IntRange(5, 21600), default=1800, show_default=True, help='push时, 从云端同步到集群的连接超时时间, 单位(s)')
@click.option('--cloud_connect_timeout', required=False, is_flag=False, type=click.IntRange(60, 43200), default=120, show_default=True, help='push时, 从本地上传分片到云端的超时时间, 单位(s)')
@click.option('--token_expires', required=False, is_flag=False, type=click.IntRange(900, 43200), default=1800, show_default=True, help='push时, 从本地上传到云端的sts token有效时间, 单位(s)')
@click.option('--part_mb_size', required=False, is_flag=False, type=click.IntRange(10, 10240), default=100, show_default=True, help='push时, 从本地上传到云端的分片大小, 单位(MB)')
@click.option('--proxy', required=False, is_flag=False, default='', help='push时, 从本地上传到云端时使用的代理url')
@click.option('--options', type=str, multiple=True, help='指定任务的一些可选项，具体看 Extra Cluster Options')
async def python(experiment_type, experiment_py, experiment_args, name, nodes, priority,
                 group, image, environments, suspend_seconds, life_state, no_inherit, follow,
                 force, no_checksum, no_hfignore, no_zip, no_diff, list_timeout, sync_timeout,
                 cloud_connect_timeout, token_expires, part_mb_size, proxy, options):
    if cmd != 'exec' and not os.path.exists(experiment_py):
        print(f'{experiment_py} 文件不存在')
        return
    if experiment_type in [JobType.LOCAL, JobType.SIMULATE]:
        func_python_local(experiment_py, experiment_args, suspend_seconds, life_state)
    else:
        await func_python_cluster(experiment_py, experiment_args, name, nodes, priority, group, image, environments,
                                  no_inherit, follow, force, no_checksum, no_hfignore, no_zip, no_diff, list_timeout,
                                  sync_timeout, cloud_connect_timeout, token_expires, part_mb_size, proxy, options)
