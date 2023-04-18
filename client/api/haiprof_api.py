from .api_utils import async_requests, RequestMethod
from .api_config import get_mars_token as mars_token
from .api_config import get_mars_url as mars_url


async def create_haiprof_task(task_id, options):
    return await async_requests(RequestMethod.POST, url=f'{mars_url()}/operating/task/haiprof?token={mars_token()}&id={task_id}',
                                assert_success=[1], json=options)
