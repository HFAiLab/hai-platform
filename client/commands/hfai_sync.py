

import os
import asyncclick as click
from rich.console import Console

from .utils import HandleHfaiCommandArgs


@click.command(cls=HandleHfaiCommandArgs)
@click.argument('src')
@click.argument('dist')
async def sync(src, dist):
    """
    同步文件 src -> dist
    """
    console = Console()
    os.system(f"rsync --progress -avzu {src} {dist} --delete")
    console.print("")
    console.print(f"从 {src} 同步到 {dist} 完成（目前直接采用 rsync）")
