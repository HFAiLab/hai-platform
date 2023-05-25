
from fastapi import Depends, HTTPException

from api.depends import get_api_task, get_internal_api_user_with_token
from base_model.training_task import TrainingTask
from server_model.user import User
from utils import run_cmd_aio
from conf import CONF
from logm import logger

# TODO(role): 是否开放 nodeport

async def node_port_svc(task: TrainingTask = Depends(get_api_task()),
                          usage: str = 'ssh', rank: int = 0,
                          dist_port: int = 22, user: User = Depends(get_internal_api_user_with_token())):
    assert rank >= 0
    try:
        result = await user.nodeport.async_create(task=task, alias=usage, dist_port=dist_port, rank=rank)
    except Exception as exception:
        logger.exception(exception)
        raise HTTPException(status_code=400, detail={'success': 0, 'msg': str(exception)})
    else:
        result['msg'] = '端口已经暴露' if result['existed'] else '暴露端口成功'
        del result['existed']
        return {'success': 1, 'result': result}


async def delete_node_port_svc(task: TrainingTask = Depends(get_api_task()),
                               rank: int = 0, dist_port: int = 22,
                               usage: str = None,   # `usage` is deprecated
                               user: User = Depends(get_internal_api_user_with_token())):
    assert rank >= 0
    try:
        await user.nodeport.async_delete(task, dist_port=dist_port, rank=rank)
    except Exception as exception:
        logger.exception(exception)
        raise HTTPException(status_code=400, detail={'success': 0, 'msg': str(exception)})
    else:
        return {'success': 1, 'msg': '删除成功'}


async def get_node_port_svc_list(user: User = Depends(get_internal_api_user_with_token())):
    return {'success': 1, 'result': await user.nodeport.async_get()}


async def task_ssh_ip(pod_name: str, user: User = Depends(get_internal_api_user_with_token())):
    ip = (await run_cmd_aio(f"kubectl -n {user.config.task_namespace} get pod {pod_name} -o wide | grep -v NAME | awk '{{print $6}}'"))[0].decode().replace("\n", "")
    return {
            'success': 1,
            'ip': ip
        }
