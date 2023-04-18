import treelib
from rich import box
from rich.table import Table
from rich.console import Console

from .api_config import get_mars_url as mars_url
from .api_config import get_mars_token as mars_token
from .api_utils import async_requests, RequestMethod


async def get_cluster_info(**kwargs):
    """
    获取集群信息
    :return: cluster_df
    """
    token = kwargs.get('token', mars_token())
    cluster_df = await async_requests(method=RequestMethod.POST, url=f'{mars_url()}/query/node/list?token={token}')
    try:
        cluster_df = cluster_df['cluster_df']
    except Exception:
        raise ValueError('mars api /cluster_df did not return correctly')
    return cluster_df


async def show_nodes(tree_format):
    df = await get_cluster_info()
    if tree_format:
        tree = treelib.Tree()
        root = 'marsV2'
        tree.create_node(root, root)
        for node in df:
            if node['mars_group']:
                group_levels = node['mars_group'].split('.')
                if group_levels[0] not in tree.nodes:
                    tree.create_node(group_levels[0], group_levels[0], root)
                for index, tree_node in enumerate(group_levels):
                    if index != len(group_levels) - 1:
                        identifier = '.'.join(group_levels[:index + 2])
                        if identifier not in tree.nodes:
                            tree.create_node(group_levels[index + 1], identifier, parent='.'.join(group_levels[:index + 1]))
                tree.create_node(node['name'], node['name'], parent=node['mars_group'])
            else:
                tree.create_node(node['name'], node['name'], root)
        tree.show()
    else:
        resource_table = Table(show_header=True, box=box.SQUARE_DOUBLE_HEAD)
        resource_columns = ['node', 'status', 'group', 'cluster']
        for k in resource_columns:
            resource_table.add_column(k)

        def node_status(d):
            if d['working'] is not None:
                return '[yellow]working[/yellow]'
            if d['status'] == 'Ready':
                return '[blue]ready[/blue]'
            return '[red]invalid[/red]'
        rows = []
        for item in df:
            rows.append([item['name'], node_status(item), item['mars_group'], item['cluster']])
        rows.sort()
        for row in rows:
            resource_table.add_row(*row)
        console = Console()
        console.print(resource_table)
