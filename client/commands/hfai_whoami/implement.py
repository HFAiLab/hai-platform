
from .default import *
from .custom import *

import asyncclick as click
from rich import box
from rich.layout import Layout

from .common import BOX_TYPE, create_data_tasks
from ..utils import HandleHfaiCommandArgs
from ...model.utils import get_current_user


async def prepare_profile_data_tasks():
    global data_tasks
    user = get_current_user()
    create_data_tasks(get_components(user))
    await user.async_get_info()


async def prepare_layout():
    await prepare_profile_data_tasks()
    layout = Layout()
    layout.split_row(Layout(name="main", size=120), Layout(visible=False))
    main = layout['main']
    await split_main_layout(main)
    main['content'].split_row(
        Layout(name='left', size=60),
        Layout(name='right', size=60)
    )
    await fill_content(main['content'])
    return layout


@click.command(cls=HandleHfaiCommandArgs)
@click.option('-a', '--ascii', required=False, is_flag=True, default=False, help='使用 ASCII 字符打印制表符, 格式错乱时请开启此选项')
async def whoami(ascii):
    """
    显示用户的个人信息, 包括集群用量、quota 等
    """
    if ascii:
        BOX_TYPE.box_table = box.ASCII_DOUBLE_HEAD
        BOX_TYPE.box_panel = box.ASCII
    console = init_console()
    with console.status('Loading data ...'):
        main = await prepare_layout()
    console.print(main)
