

from .api_config import get_mars_url as mars_url
from .training_api import task_id
from .api_config import get_mars_token
from .api_utils import RequestMethod, request_url


def set_swap_memory(swap_limit: int):
    token = get_mars_token()
    t_id = task_id()
    assert int(t_id) > 0, "需要是集群训练任务环境"
    result = request_url(RequestMethod.POST, f'{mars_url()}/ugc/swap_memory?token={token}&id={t_id}&swap_limit={swap_limit}')
    return result['result']
