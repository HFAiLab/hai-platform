import signal
import sys
import asyncio
import getpass
import json
import math
import os
import munch
import yaml

import asyncclick as click
from rich import box
from rich.console import Console
from rich.table import Table

from hfai.client import get_experiments, create_experiment
from hfai.client.api import Experiment, get_task_ssh_ip, get_task_container_log
from hfai.client.commands.hfai_artifact import map_artifact
from hfai.client.model.utils import get_current_user
from hfai.conf.flags import STOP_CODE, CHAIN_STATUS
from .utils import HandleHfaiCommandArgs, console_out_experiment, \
    ExperimentEncoder, get_experiment_auto, CLI_NAME


class HandleHfaiCommandArgsWithExpArg(HandleHfaiCommandArgs):
    def format_options(self, ctx, formatter):
        pieces = self.collect_usage_pieces(ctx)
        with formatter.section("Arguments"):
            if 'experiment' in pieces:
                formatter.write_dl(rows=[('experiment', '用于检索的任务，可以是任务名、是任务ID，也可以是提交的任务配置文件')])
            if 'experiment.yml' in pieces:
                formatter.write_dl(rows=[('experiment.yml', '创建任务的任务配置文件')])
        super(HandleHfaiCommandArgsWithExpArg, self).format_options(ctx, formatter)


@click.command(cls=HandleHfaiCommandArgsWithExpArg)
@click.argument('experiment', metavar='experiment')
@click.option('-j', '--json', 'print_json', required=False, is_flag=True, default=False, help='将任务状态转换成 json 格式，是否以json形式查看')
@click.option('-t', '--exp_type', default='auto', help='配合 <experiment> 使用，默认 auto 会尝试解析')
async def status(experiment, print_json, exp_type, print_out=True):
    """
    查询任务状态
    """
    console = Console()
    experiment = await get_experiment_auto(experiment, exp_type)
    if print_json:
        rst = json.dumps(obj=experiment.__dict__['_trait_values'], ensure_ascii=False, cls=ExperimentEncoder)
        print(rst)
        return rst
    if print_out:
        console_out_experiment(console, experiment)
    return experiment


describe_help = f"""
    打印任务的 schema yaml，可以在下次创建任务的时候使用

    {CLI_NAME} describe <task> > /tmp/task.yml; {CLI_NAME} run /tmp/task.yml

    """

@click.command(cls=HandleHfaiCommandArgsWithExpArg, help=describe_help)
@click.argument('experiment', metavar='experiment')
@click.option('-t', '--exp_type', default='auto', help='配合 <experiment> 使用，默认 auto 会尝试解析')
async def describe(experiment, exp_type, print_out=True):
    console = Console()
    experiment = await get_experiment_auto(experiment, exp_type)
    schema = experiment.schema
    if print_out:
        console.print(yaml.dump(schema))
    return schema


@click.command(cls=HandleHfaiCommandArgsWithExpArg)
@click.argument('experiment', metavar='experiment')
@click.option('-f', '--follow', required=False, is_flag=True, default=False, help='追加查看日志')
@click.option('-r', '--rank', default=0, help='指定查看第几个节点的日志')
@click.option('-t', '--exp_type', default='auto', help='配合 <experiment> 使用，默认 auto 会尝试解析')
@click.option('-c', '--container', required=False, is_flag=True, default=False, help='查看 container 的日志（非任务日志），--follow 会失效')
async def logs(experiment, follow, rank, exp_type, container=False):
    """
    查看任务日志
    """
    def signal_handler(sig, frame):
        print('停止追加查看日志，任务会继续运行...')
        sys.exit(0)

    def check_hfai_print(log_line):
        hfai_print_pattern = '[HFAI_PRINT:'
        if log_line[29:29+len(hfai_print_pattern)] == hfai_print_pattern:
            rest_log = log_line[29+len(hfai_print_pattern):]
            hfai_print_type = rest_log[:rest_log.find(']')]
            return hfai_print_type, log_line[:29], log_line[29+len(hfai_print_pattern)+len(hfai_print_type)+1:]
        return '', log_line[:29], log_line[29:]

    signal.signal(signal.SIGINT, signal_handler)

    experiment = await status.callback(experiment, False, exp_type)
    console = Console()
    if container:
        console.print('=' * 20 + f' [blue] fetching [/blue] container log on rank {rank}... ' + '=' * 20)
        container_log = await get_task_container_log(id=experiment.id, rank=rank)
        console.out(container_log, end="")
        console.out('')
    else:
        console.print('=' * 20 + f' [blue] fetching [/blue] log on rank {rank}... ' + '=' * 20)
        hf_tqdm = False
        while True:
            log, exit_code, stop_code = await experiment.log(rank=rank, last_seen=json.dumps(experiment.last_seen), with_code=True)
            log = '' if log == '还没产生日志' else log
            # console.out(log, end='')
            log_lines = log.split('\n')
            if log_lines[-1] == '':
                log_lines = log_lines[:-1]
            for log_line_idx, log_line in enumerate(log_lines):
                hfai_print_type, log_line_timestamp, log_line_content = check_hfai_print(log_line)
                if hfai_print_type == 'TQDM':
                    hf_tqdm = True
                    console.out(log_line_timestamp + log_line_content, end='\r')
                    continue
                if hf_tqdm:
                    hf_tqdm = False
                    console.out('')
                if hfai_print_type == 'IMAGE':
                    console.out(log_line_timestamp + '[这里是用户打印的一张图片，cmd line 无法展示]')
                    continue
                # fix rich 不处理 \r 的问题
                segments = log_line.split('\r')
                for idx, seg in enumerate(segments):
                    console.out(seg, end='\r' if idx + 1 < len(segments) else '\n')
            # 如果 stop_code 不是被打断的，那么 exit
            if stop_code < STOP_CODE.HOOK_RESTART:
                if stop_code == STOP_CODE.STOP:
                    sys.exit(0)
                if stop_code >= STOP_CODE.FAILED:
                    sys.exit(exit_code or 1)
            # 其他的任务就算 stop 了，也没跑完，等着调度

            if follow:
                await asyncio.sleep(11)
            else:
                break


@click.command(cls=HandleHfaiCommandArgsWithExpArg)
@click.argument('experiment_yml', metavar='experiment.yml')
@click.option('-f', '--follow', required=False, is_flag=True, default=False, help='追加查看日志')
@click.option('-n', '--nodes', type=int, help='用多少个节点跑')
@click.option('-g', '--group', help='任务跑在哪个分组')
@click.option('-p', '--priority', type=int, help='任务优先级，从低到高: 20, 30, 40, 50')
@click.option('-i', '--input', required=False, default='', help='任务输入artifact，格式为name:version')
@click.option('-o', '--output', required=False, default='', help='任务输出artifact, 格式为name:version')
async def run(experiment_yml, follow, nodes, group, priority, input, output):
    """
    根据 yaml 文件来运行一个任务，可以通过参数来覆盖配置; nodes、group、priority 参数可以覆盖 yml 里面的配置
    """
    console = Console()

    if not isinstance(experiment_yml, munch.Munch):
        assert os.path.exists(experiment_yml), '配置文件不存在'
        config_yml = munch.Munch.fromYAML(open(experiment_yml))
    else:  # hfai python 的时候，这个是一个 munch 实例
        config_yml = experiment_yml
    assert config_yml.get('version', None) == 2, '请使用 create_experiment 的配置文件'
    if group:
        config_yml.resource.group = group
    if priority:
        config_yml.priority = priority
    if nodes:
        config_yml.resource.node_count = nodes
    experiment = await create_experiment(config_yml)
    experiment_table, job_table = experiment.tables()
    console.print('=' * 20 + ' experiment ' + '=' * 20)
    console.print(experiment_table)
    console.print(f'任务创建完成，请等待调度，可以使用以下接口查询\n'
                  f'   {CLI_NAME} status {experiment.nb_name}  # 查看任务状态\n'
                  f'   {CLI_NAME} logs -f {experiment.nb_name} # 查看任务日志\n'
                  f'   {CLI_NAME} stop {experiment.nb_name} # 关闭任务\n')
    try:
        if input:
            await map_artifact.callback(experiment.nb_name, input, 'input')
        if output:
            await map_artifact.callback(experiment.nb_name, output, 'output')
    except Exception as e:
        console.print(f'设置artifact失败 {str(e)}, 请检查后通过hfai artifact子命令手动设置')
    if follow:
        console.print('')
        await logs.callback(experiment.id, follow, 0, 'id')


@click.command(cls=HandleHfaiCommandArgs)
@click.option('-p', '--page', type=int, default=1, help='列出用户任务列表, 用户要查看第几页的任务列表')
@click.option('-ps', '--page_size', type=int, default=12, help='用户指定一页的任务列表有多少条任务')
async def exp_list(page, page_size):
    """
     列出用户任务列表, 用户要查看第几页的任务列表
    """
    total, experiments = await get_experiments(page, page_size,
                                               select_pods=False)
    experiment_table = Table(show_header=True, box=box.ASCII_DOUBLE_HEAD)
    nb_names = set()
    running_nb_names = set()
    for k in Experiment.experiment_columns:
        experiment_table.add_column(k)
    for e in experiments:
        if e.chain_status != CHAIN_STATUS.FINISHED:
            running_nb_names.add(e.nb_name)
        nb_names.add(e.nb_name)
        experiment_table.add_row(*e.row())
    console = Console()
    console.print(
        f'现在查看的是第 {page} 页任务，共 {math.ceil(total / int(page_size))} 页,'
        f' 每页 {page_size} 个任务, 共 {total} 个任务')
    console.print(experiment_table)


@click.command(cls=HandleHfaiCommandArgsWithExpArg)
@click.argument('experiment', metavar='experiment')
@click.option('-t', '--exp_type', default='auto', help='配合 <experiment> 使用，默认 auto 会尝试解析')
@click.option('--succeeded', is_flag=True, default=False, help='把任务状态写为 succeeded')
@click.option('--failed', is_flag=True, default=False, help='把任务状态写为 failed')
async def stop(experiment, exp_type, succeeded=False, failed=False):
    """
    关闭任务状态
    """
    experiment = await get_experiment_auto(experiment, exp_type)
    console = Console()
    op = 'stop'
    if failed and succeeded:
        console.print('状态不能同时设置为 succeeded 和 failed！')
        sys.exit(1)
    elif failed:
        op = 'fail'
    elif succeeded:
        op = 'succeed'
    await experiment.stop(op=op)


@click.command(cls=HandleHfaiCommandArgsWithExpArg)
@click.argument('experiment', metavar='experiment')
@click.option('-r', '--rank', default=0, help='指定登录到哪台机器')
@click.option('-t', '--exp_type', default='auto', help='配合 <experiment> 使用，默认 auto 会尝试解析')
async def ssh(experiment, rank, exp_type):
    """
    登录到哪台机器，只能在开发容器内使用
    """
    # 只能从集群内部 ssh
    experiment = await get_experiment_auto(experiment, exp_type)
    pod_name = f'{getpass.getuser()}-{experiment.id}-{rank}'
    ip = await get_task_ssh_ip(pod_name=pod_name)
    cmd = f'ssh -o ServerAliveInterval=60 -o StrictHostKeyChecking=no {ip}'
    if not get_current_user().is_internal:
        cmd += ' -p 20022'
    if not ip:
        print("没有正确获取到这个任务的ip，可能已经停止了")
        sys.exit()
    print('ssh cmd:', cmd)
    os.system(cmd)
