from .api_config import get_mars_url as mars_url
from .api_config import get_mars_token as mars_token
from .api_utils import async_requests, RequestMethod


async def get_tasks_overview(**kwargs):
    """
    获取集群任务的概况信息列表
    """
    token = kwargs.get('token', mars_token())

    url = f'{mars_url()}/query/task/list_all_with_priority?token={token}'
    result = await async_requests(RequestMethod.POST, url)
    return result['result']


async def get_cluster_overview(**kwargs):
    """
    获取集群概况 2022.06.20 之后的版本
    """
    token = kwargs.get('token', mars_token())
    url = f'{mars_url()}/query/node/client_overview?token={token}'
    result = await async_requests(RequestMethod.POST, url)
    return result['result']
