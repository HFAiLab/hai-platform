import sys

from rich import box
from rich.table import Table
from rich.console import Console

from .api_config import get_mars_token as mars_token
from .api_config import get_mars_url as mars_url
from .api_utils import async_requests, RequestMethod
from ..model.user import User


def get_user(**kwargs) -> User:
    return User(token=kwargs.get('token', mars_token()))


async def async_get_artifact(name, version='default', page=1, page_size=1000, **kwargs):
    user = get_user(**kwargs)
    try:
        ret = await user.artifact.async_get_artifact(name, version, page, page_size)
        table = Table('owner', 'name', 'version', 'type', 'location', 'description', 'extra', 'shared', show_header=True, box=box.SQUARE_DOUBLE_HEAD)
        for row in ret['msg']:
            values = list(row.values())
            values[7] = 'N' if values[7] == values[0] else 'Y'
            table.add_row(*[v if v else ' - ' for v in values])
        console = Console()
        console.print(table)
    except Exception as e:
        print(f'获取artifact {name}:{version}失败: {e}')
        sys.exit(1)


async def async_create_update_artifact(name,
                                       version='default',
                                       type='',
                                       location='',
                                       description='',
                                       extra='',
                                       private=False,
                                       **kwargs):
    if ':' in name or ':' in version:
        print('artifact name, version不能包含":"')
        sys.exit(1)
    user = get_user(**kwargs)
    try:
        ret = await user.artifact.async_create_update_artifact(name, version, type, location, description, extra, private)
        print(ret['msg'])
    except Exception as e:
        print(f'更新artifact {name}:{version}失败: {e}')
        sys.exit(1)


async def async_delete_artifact(name, version='default', **kwargs):
    user = get_user(**kwargs)
    try:
        ret = await user.artifact.async_delete_artifact(name, version)
        print(ret['msg'])
    except Exception as e:
        print(f'删除artifact {name}:{version}失败: {e}')
        sys.exit(1)


async def async_get_all_task_artifact_mapping(artifact_name, artifact_version, page=1, page_size=1000, days=90, **kwargs):
    """
    获取当前任务制品信息
    """
    token = kwargs.get('token', mars_token())
    try:
        url = f'{mars_url()}/query/task/artifact/list?artifact_name={artifact_name}&artifact_version={artifact_version}&page={page}&page_size={page_size}&days={days}&token={token}'
        ret = await async_requests(RequestMethod.POST, url, [1])
        table = Table('user', 'chain_id', 'nb_name', 'task_ids', 'in_artifact', 'out_artifact', show_header=True, box=box.SQUARE_DOUBLE_HEAD)
        for row in ret['msg']:
            values = list(row.values())
            values[3] = ','.join([str(i) for i in values[3]])
            table.add_row(*[v if v else ' - ' for v in values])
        console = Console()
        console.print('现存映射关系：')
        console.print(table)
        return len(ret['msg'])
    except Exception as e:
        print(f'获取artifact失败: {e}')
        sys.exit(1)
