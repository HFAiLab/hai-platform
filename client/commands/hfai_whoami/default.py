from rich.console import Console
from rich.layout import Layout

from .common import node_quota_section, access_section, jupyter_quota_section, profile_section, PROFILE_HEIGHT


console_height = 30


def init_console():
    return Console(height=console_height)


def get_components(user):
    return ['quota', 'image']


async def split_main_layout(main):
    main.split_column(
        Layout(await profile_section(), size=PROFILE_HEIGHT),
        Layout(name='content', size=console_height - PROFILE_HEIGHT),
    )
    main['content'].split_row(
        Layout(name='left', size=60),
        Layout(name='right', size=60)
    )


async def fill_content(content):
    height = console_height - PROFILE_HEIGHT
    left, right = content['left'], content['right']

    left.split_column(Layout(await node_quota_section(height=height), size=height))
    right.split_column(Layout(await access_section(height=8), size=8),
                       Layout(await jupyter_quota_section(height=height-8), size=height-8))
