import json
import os
import sys
import math
import munch
import asyncclick as click
from datetime import datetime

from asyncclick import Context, HelpFormatter
from hfai.base_model.base_task import BasePod
from hfai.client.api import Experiment
from hfai.client import get_experiment


CLI_NAME = os.path.basename(sys.argv[0])
if 'sphinx' in CLI_NAME:
    CLI_NAME = 'hai-cli'
is_hai = 'hai' in CLI_NAME

# ~/.hfai/plugins 为 plugins配置文件，每行可为plugin执行文件路径，或者plugin名(将在python bin目录寻找对应文件)
# 示例：
# $ cat ~/.hfai/plugins
# haienv
# /usr/local/bin/haiworkspace

# 注：1. 需要先保证对应 plugin 已安装
#     2. plugin执行文件命名规则：f'hai{subcommand}', 可直接执行，也可以通过 argv_0(如hfai) 执行，对应子命令为： argv_0 subcommand <OPTIONS>
SYS_BIN_PATH = '/usr/local/bin' if sys.prefix == '/usr' else f'{sys.prefix}/bin'
PLUGIN_LIST = {
    'haiworkspace': f'{SYS_BIN_PATH}/haiworkspace',
    'haienv': f'{SYS_BIN_PATH}/haienv',
}
try:
    with open(os.path.expanduser(os.environ.get('HAI_PLUGIN_CONFIG', '~/.hfai/plugins'))) as f:
        for line in f.readlines():
            path = os.path.expanduser(line.strip())
            basename = os.path.basename(path)
            if basename.startswith('hai') and not basename.startswith('hai-') and basename != CLI_NAME:
                PLUGIN_LIST[basename] = path if os.sep in path else f'{SYS_BIN_PATH}/{basename}'
except:
    pass


class HandleHfaiGroupArgs(click.Group):
    def format_usage(self, ctx, formatter):
        pieces = ['COMMAND', '<argument>...', '[OPTIONS]']
        formatter.write_usage(ctx.command_path, " ".join(pieces))

    def format_commands(self, ctx: Context, formatter: HelpFormatter) -> None:
        exec_commands = []
        manage_commands = []
        cluster_commands = []
        user_commands = []
        ugc_commands = []
        other_commands = []
        for subcommand in self.list_commands(ctx):
            cmd = self.get_command(ctx, subcommand)
            # What is this, the tool lied about a command.  Ignore it
            if cmd is None:
                continue
            if cmd.hidden:
                continue
            if subcommand in ['bash', 'python', 'exec', 'run']:
                _commands = exec_commands
            elif subcommand in ['list', 'logs', 'stop', 'status', 'ssh', 'describe']:
                _commands = manage_commands
            elif subcommand in ['images', 'venv', 'workspace']:
                _commands = ugc_commands
            elif subcommand in ['init', 'whoami']:
                _commands = user_commands
            elif subcommand in ['monitor', 'nodes', 'prof', 'validate']:
                _commands = cluster_commands
            else:
                _commands = other_commands
            _commands.append((subcommand, cmd))

        for name, cmds in zip(['Exec Commands', 'Task Manage Cmds', 'Cluster Commands', 'User Commands', 'UGC Commands', 'Other Commands'],
                              [exec_commands, manage_commands, cluster_commands, user_commands,  ugc_commands, other_commands]):
            if len(cmds) > 0:
                rows = []
                for subcommand, cmd in cmds:
                    help = cmd.get_short_help_str()
                    if subcommand == 'bash':
                        help = '在集群上运行 bash 脚本'
                    elif subcommand == 'python':
                        help = '在集群上运行 bash 脚本'
                    elif subcommand == 'exec':
                        help = '在集群上运行 二进制 文件'
                    rows.append((subcommand, help))

                if rows:
                    with formatter.section(name):
                        formatter.write_dl(rows)


class HandleHfaiCommandArgs(click.Command):
    def format_usage(self, ctx, formatter):
        pieces = self.collect_usage_pieces(ctx)
        pieces = [f'<{p}>' for p in pieces[1:]] + ['[OPTIONS]']
        formatter.write_usage(ctx.command_path, " ".join(pieces))


class HandleHfaiPluginCommandArgs(click.Command):
    async def parse_args(self, ctx: Context, args):
        if len(args) == 0 or '-h' in args or '--help' in args:
            cmd = f"{PLUGIN_LIST[ctx.command_path.replace(f'{CLI_NAME} ', 'hai')]} {' '.join(args)}"
            msg = os.popen(cmd).read().strip().replace('hai', f'{CLI_NAME} ')
            from asyncclick.utils import echo
            echo(msg, color=ctx.color)
            ctx.exit()

        ctx.resilient_parsing = True
        return await super().parse_args(ctx, args)


async def func_experiment_auto(func, experiment, exp_type='auto', **kwargs):
    exp: str = experiment
    e_type = exp_type
    valid_type = ['auto', 'yaml', 'name', 'id']
    assert e_type in valid_type, f'exp_type 必须是 {valid_type} 其中一个，现在是 {e_type}'
    if e_type == 'auto':
        if exp.isnumeric():
            e_type = 'id'
        elif os.path.exists(exp) and any(exp.endswith(suffix) for suffix in ['.yaml', '.yml']):
            e_type = 'yaml'
        else:
            e_type = 'name'

    if e_type == 'id':
        return await func(id=int(exp), **kwargs)
    elif e_type == 'yaml':
        exp_config = munch.Munch.fromYAML(open(exp))
        try:
            exp = exp_config.name
        except:
            # 不是合法的 yaml，当作 e_type = 'name' 解析
            pass

    return await func(name=exp, **kwargs)


def console_out_experiment(console, experiment: Experiment):
    experiment_table, job_table = experiment.tables()
    console.print('=' * 20 + ' experiment ' + '=' * 20)
    console.print(experiment_table)
    console.print('=' * 20 + ' jobs ' + '=' * 20)
    console.print(job_table)


class ExperimentEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, BasePod):
            return obj.__dict__
        else:
            return json.JSONEncoder.default(self, obj)


async def get_experiment_auto(experiment: str, exp_type: str) -> Experiment:
    exp = experiment
    experiment = await func_experiment_auto(get_experiment, exp, exp_type)
    return experiment


number_units = ['', 'K', 'M', 'G', 'T', 'P']
def human_readable(number: int, format="%.2f"):
    if number == 0: return format % 0
    sign, number = number/math.fabs(number), math.fabs(number)
    magnitude = min(len(number_units)-1, int(math.floor(math.log(number, 1000))))
    return f'{format}' % (sign * number / 1000**magnitude) + number_units[magnitude]


def get_workspace_conf(cwd):
    """
    获取这个目录的 workspace 配置文件，返回 workspace config 的文件
    @return: workspace_config_file
    """
    subs = []
    workspace_config_file = './.hfai/workspace.yml'
    while cwd != '/':
        wcf = os.path.join(cwd, workspace_config_file)
        if os.path.exists(wcf):
            return wcf, subs
        subs.insert(0, os.path.basename(cwd))
        cwd = os.path.dirname(cwd)
    return None, None
