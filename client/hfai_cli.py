import os
import asyncclick as click
import sys
argv_0 = sys.argv[0]

from hfai.client.commands.hfai_experiment import run, exp_list, status, stop, \
    ssh, logs, describe
from hfai.client.commands.hfai_init import init
from hfai.client.commands.hfai_nodes import nodes
from hfai.client.commands.hfai_python import python
from hfai.client.commands.utils import HandleHfaiGroupArgs, HandleHfaiPluginCommandArgs, PLUGIN_LIST
from hfai.client.commands.hfai_image import images
from hfai.client.commands.hfai_whoami import whoami
vars_before_custom = dir()

try:
    from hfai.client.commands.custom import *
    custom_vars = set(dir()) - set(vars_before_custom) - {'vars_before_custom'}
except ImportError:
    custom_vars = []

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

try:
    from hfai import __version__
except:
    try:
        import hfai.version
        __version__ = hfai.version.__version__
    except:
        __version__ = 'undefined'


def my_except_hook(exctype, value, traceback):
    import aiohttp
    import requests
    if exctype == requests.exceptions.ConnectionError or exctype == aiohttp.client_exceptions.ClientConnectorError:
        print(value)
        print('请求集群服务出现连接错误，请检查您的网络连接')
        sys.exit(1)
    else:
        sys.__excepthook__(exctype, value, traceback)


@click.group(cls=HandleHfaiGroupArgs, context_settings=CONTEXT_SETTINGS)
@click.version_option(version=__version__)
async def cli():
    sys.excepthook = my_except_hook  # 这样不会影响任务调用接口


cli.add_command(init)
cli.add_command(python)
cli.add_command(python, 'bash')
cli.add_command(python, 'exec')
cli.add_command(status)
cli.add_command(stop)
cli.add_command(logs)
cli.add_command(exp_list, 'list')
cli.add_command(describe)
cli.add_command(nodes)
cli.add_command(whoami)
cli.add_command(run)
cli.add_command(ssh)
cli.add_command(images)

for var in custom_vars:
    cli.add_command(globals()[var])

# 增加plugin命令
for plugin, plugin_path in PLUGIN_LIST.items():
    subcommand = plugin[3:]
    def exec_func():
        cmd = f"{plugin_path} {' '.join(sys.argv[2:])}" if len(sys.argv) > 2 else plugin_path
        def exec_func_inner():
            if os.system(cmd):
                sys.exit(1)
        # cli help text
        exec_func_inner.__doc__ = plugin
        return exec_func_inner
    exec_func_cmd = click.command(cls=HandleHfaiPluginCommandArgs)(exec_func())
    cli.add_command(exec_func_cmd, subcommand)


if __name__ == '__main__':
    cli(_anyio_backend='asyncio')
