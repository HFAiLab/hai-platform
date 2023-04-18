

from .api_config import get_mars_token as mars_token
from .api_config import get_mars_url as mars_url
from .api_utils import async_requests, RequestMethod
from .training_api import task_id, current_selector_task
from ..model import get_current_user
from hfai.base_model.base_task import BaseTask


async def get_task_ssh_ip(pod_name: str, **kwargs):
    assert pod_name is not None, '必须指定 pod_name'
    token = kwargs.get('token', mars_token())
    url = f'{mars_url()}/query/task/ssh_ip?pod_name={pod_name}&token={token}'
    result = await async_requests(RequestMethod.POST, url)
    return result['ip']


async def create_node_port_svc(usage: str, dist_port: int, **kwargs):
    assert usage is not None, '必须指定 usage'
    assert dist_port is not None, '必须指定 dist_port'
    task = current_selector_task()
    assert int(task.id) > 0, "需要是集群训练任务环境"
    result = await get_current_user().nodeport.async_create(task=task, alias=usage, dist_port=dist_port)
    return result['result'] if result['success'] else result


async def delete_node_port_svc(dist_port: int, usage: str = None, nb_name: str = None, **kwargs):
    assert dist_port is not None, '必须指定 dist_port'
    assert int(task_id()) > 0, "需要是集群训练任务环境"
    # 未指定容器的 nb_name 则删除当前任务下的端口
    task = make_selector_task(nb_name=nb_name) if nb_name is not None else current_selector_task()
    return await get_current_user().nodeport.async_delete(task=task, dist_port=dist_port)


async def get_node_port_svc_list(**kwargs):
    return await get_current_user().nodeport.async_get()


def make_selector_task(nb_name=None, task_id=None, chain_id=None) -> BaseTask:
    """ 返回一个只有任务选择所需属性的 task, 用于 API 调用 """
    task = BaseTask()
    task.nb_name = nb_name
    task.id = task_id
    task.chain_id = chain_id
    return task
