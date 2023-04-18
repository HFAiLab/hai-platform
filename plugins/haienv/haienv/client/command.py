import asyncclick as click
import sys
import os
from .api import create_haienv, list_haienv, remove_haienv
from rich import box
from rich.console import Console
from rich.table import Table
from rich.box import ASCII2
from .model import get_db_path, get_path_prefix, Haienv
import getpass
import json


class HandleHfaiGroupArgs(click.Group):
    def format_usage(self, ctx, formatter):
        pieces = ['COMMAND', '<argument>...', '[OPTIONS]']
        formatter.write_usage(ctx.command_path, " ".join(pieces))


class HaienvHandleHfaiCommandArgs(click.Command):
    def format_options(self, ctx, formatter):
        with formatter.section("Arguments"):
            formatter.write_dl(rows=[('haienv_name', 'haienv的名字')])
        super(HaienvHandleHfaiCommandArgs, self).format_options(ctx, formatter)


@click.command(cls=HaienvHandleHfaiCommandArgs)
@click.argument('haienv_name', required=True, metavar='haienv_name')
@click.option('--no_extend', required=False, is_flag=True, default=False, help='扩展当前python环境（默认为扩展），注意扩展当前环境极有可能出现版本兼容问题')
@click.option('-p', '--py', default=os.environ.get('HAIENV_PY', '.'.join([str(i) for i in sys.version_info[:3]])), help='选择python版本，默认为当前python版本')
@click.option('--extra-search-dir', required=False, type=str, multiple=True, help='指定在进入该虚拟环境时额外的pythonpath')
@click.option('--extra-search-bin-dir', required=False, type=str, multiple=True, help='指定在进入该虚拟环境时额外的path')
@click.option('--extra-environment', required=False, type=str, multiple=True, help='指定在进入该虚拟环境时额外的环境变量')
async def create(haienv_name, no_extend, py, extra_search_dir, extra_search_bin_dir, extra_environment):
    """
    使用conda创建新的虚拟环境，注意必须有conda并配置好相应代理（如有需要）

    eg. haienv create my_env --no_extend --py 3.6 --extra-search-dir /tmp/123 --extra-search-dir /tmp/456 --extra-environment TEMP=temp
    """
    print(f"当前虚拟环境目录为{get_path_prefix()}，如需更改请设置环境变量HAIENV_PATH", flush=True)
    assert os.popen('uname').read().strip() == 'Linux', 'haienv只支持Linux环境'
    assert os.popen('/usr/local/cuda/bin/nvcc -V 2>/dev/null || nvcc -V 2>/dev/null').read(), '未找到/usr/local/cuda/bin/nvcc 以及 nvcc，请设置环境变量PATH'
    assert any(v in os.popen('/usr/local/cuda/bin/nvcc -V 2>/dev/null || nvcc -V 2>/dev/null').read() for v in ['11.1', '11.3']), '目前haienv只支持cuda 11.1和cuda 11.3'
    result = await create_haienv(haienv_name=haienv_name, extend=('False' if no_extend else 'True'), py=py, extra_search_dir=extra_search_dir, extra_search_bin_dir=extra_search_bin_dir, extra_environment=extra_environment)
    print(result['msg'])


@click.command(cls=HaienvHandleHfaiCommandArgs)
@click.option('-u', '--user', help='指定用户，默认为所有用户')
@click.option('-a', '--all', 'show_all', required=False, is_flag=True, default=False, help='列出所有环境')
@click.option('-o', 'output_format', default='', help='输出格式，可以选择json')
async def list(user, show_all, output_format=''):
    """
    列举所有虚拟环境
    """
    assert output_format in ['', 'json'], '目前输出格式只支持json'
    root_path = os.path.realpath(os.path.join(get_db_path(), '../..'))
    all_result = []
    for _user in sorted(os.listdir(root_path)):
        if user is not None and user != _user:
            continue
        result = await list_haienv(_user)
        if len(result) == 0:
            continue
        for k, v in result.items():
            all_result.append((_user, k, v.path, v.extend, v.extend_env, v.py))
    result_dict = {'others': [], 'own': []}
    haienv_table = Table(title='其它环境', title_justify=True, box=ASCII2, style='dim', show_header=True)
    haienv_own_table = Table(title='自己创建的环境', title_justify=True, box=ASCII2, style='dim', show_header=True)
    if user is not None:
        haienv_table = Table(show_header=True, box=box.ASCII_DOUBLE_HEAD)
    haienv_key_list = ['user', 'haienv_name', 'path', 'extend', 'extend_env', 'py']
    for k in haienv_key_list:
        haienv_table.add_column(k)
        haienv_own_table.add_column(k)
    for item in all_result:
        if user is not None or item[0] != getpass.getuser():
            tp = 'others'
            haienv_table.add_row(*item)
        else:
            tp = 'own'
            haienv_own_table.add_row(*item)
        result_dict[tp].append({key: value for key, value in zip(haienv_key_list, item)})
    if output_format == '':
        print(f"当前虚拟环境目录为{get_path_prefix()}，如需更改请设置环境变量HAIENV_PATH", flush=True)
        console = Console()
        console.print(f'请在 bash 中使用以下命令加载 env: source haienv <haienv_name>')
        console.print(haienv_table)
        if user is None:
            console.print(haienv_own_table)
    if output_format == 'json':
        if user is not None:
            result_dict.pop('own', None)
        print(json.dumps(result_dict))


@click.command(cls=HaienvHandleHfaiCommandArgs)
@click.argument('haienv_name', required=True, metavar='haienv_name')
async def remove(haienv_name):
    """
    删除虚拟环境
    """
    result = await remove_haienv(haienv_name=haienv_name)
    print(result['msg'])


@click.group(cls=HandleHfaiGroupArgs)
async def config():
    """
    创建、查询、删除虚拟环境
    """
    pass


@config.command(cls=HaienvHandleHfaiCommandArgs)
@click.option('-n', '--haienv_name', required=True, help='haienv_name')
@click.option('-u', '--user', help='指定用户，默认走当前用户')
async def show(haienv_name, user):
    """
    展示指定haienv的各项参数
    """
    root_path = os.path.realpath(os.path.join(get_db_path(), f'../../{user}/venv.db')) if user is not None else get_db_path()
    haienv_config = Haienv.select(haienv_name=haienv_name, outside_db_path=root_path)
    assert haienv_config is not None, f'未找到该环境，当前虚拟环境目录为{get_path_prefix()}，请通过haienv list查看所有环境，或设置环境变量HAIENV_PATH进行更改'
    config_table = Table(show_header=True, box=box.ASCII_DOUBLE_HEAD)
    for k in ['item', 'content']:
        config_table.add_column(k)
    config_table.add_row('haienv_name', haienv_name)
    for item in ['path', 'extend', 'extend_env', 'py', 'extra-search-dir', 'extra-search-bin-dir', 'extra-environment']:
        config_table.add_row(item, f'{getattr(haienv_config, item.replace("-", "_"))}')
    console = Console()
    console.print(config_table)


@config.command(cls=HaienvHandleHfaiCommandArgs)
@click.option('-n', '--haienv_name', required=True, help='haienv_name')
@click.option('-k', '--key', required=True, help='选择清除的参数，目前只能指定extra-search-dir, extra-search-bin-dir, extra-environment中的一种')
async def clear(haienv_name, key):
    """
    清除haienv的某项参数

    eg. haienv clear -n my_env -k extra-search-dir
    """
    assert key in ['extra-search-dir', 'extra-search-bin-dir', 'extra-environment']
    key = key.replace('-', '_')
    assert Haienv.select(haienv_name=haienv_name) is not None, f'未找到该环境，当前虚拟环境目录为{get_path_prefix()}，请通过haienv list查看所有环境，或设置环境变量HAIENV_PATH进行更改'
    Haienv.update(haienv_name=haienv_name, key=key, value=[])
    print('设置成功, 目前的参数如下：')
    await show.callback(haienv_name=haienv_name, user=None)
    print(f'重新 source haienv {haienv_name} 生效该参数')


@config.command(cls=HaienvHandleHfaiCommandArgs)
@click.option('-n', '--haienv_name', required=True, help='haienv_name')
@click.option('-k', '--key', required=True, help='选择追加的参数，目前只能指定extra-search-dir, extra-search-bin-dir, extra-environment中的一种')
@click.option('-v', '--value', required=True, help='追加的参数值')
async def append(haienv_name, key, value):
    """
    追加haienv的某项参数

    eg. haienv clear -n my_env -k extra-search-dir -v /tmp/123
    """
    assert key in ['extra-search-dir', 'extra-search-bin-dir', 'extra-environment']
    key = key.replace('-', '_')
    haienv_config = Haienv.select(haienv_name=haienv_name)
    assert haienv_config is not None, f'未找到该环境，当前虚拟环境目录为{get_path_prefix()}，请通过haienv list查看所有环境，或设置环境变量HAIENV_PATH进行更改'
    Haienv.update(haienv_name=haienv_name, key=key, value=getattr(haienv_config, key) + [value])
    print('设置成功, 目前的参数如下：')
    await show.callback(haienv_name=haienv_name, user=None)
    print(f'重新 source haienv {haienv_name} 生效该参数')
