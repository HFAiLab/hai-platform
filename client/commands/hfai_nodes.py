import asyncclick as click

from .utils import HandleHfaiCommandArgs
from hfai.client.api.resource_api import show_nodes


@click.command(cls=HandleHfaiCommandArgs)
@click.option('--tree', required=False, is_flag=True, default=False, help='打印节点树状结构')
async def nodes(tree):
    """
    查看节点信息
    """
    await show_nodes(tree_format=tree)
