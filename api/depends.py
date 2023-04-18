
from typing import List

import ujson
from fastapi import Depends, HTTPException, Request
from pydantic import BaseModel

from base_model.training_task import TrainingTask
from conf.flags import TASK_TYPE, USER_ROLE
from server_model.selector import AioTrainingTaskSelector, AioUserSelector, AioBaseTaskSelector
from server_model.user import User


JUPYTER_ADMIN_GROUP = 'hub_admin'   # 能够操作其他人容器的权限组
TASK_ADMIN_GROUP = 'task_admin'     # 能够操作其他人任务的权限组


class RestTask(BaseModel):
    code_file: str
    workspace: str = ''
    token: str
    # 用于训练任务
    nb_name: str = None
    environments: dict = {}
    nodes: int = None
    group: str = None
    includes: str = ''
    template: str = 'DEFAULT'
    is_queue_job: int = 0
    priority: int = 0
    can_restart: int = 0
    task_type: str = TASK_TYPE.TRAINING_TASK
    force_restart1: int = 0
    # 用于升级任务
    upgrade_nodes: List[str] = None
    upgrade_name: str = None
    version: str = None
    include_failed: int = 0
    whole_life_state: int = 0
    mount_code: int = 2
    schedule_zone: str = None
    train_image: str = None  # 和 template 相对，指的是用户使用哪个 image 来运行
    options: dict = {}  # create_experiment 里的 options


async def request_limitation():
    if True:
        return


def get_api_user_with_token(allowed_groups=[], allowed_scopes=[], **kwargs):
    async def __func(request: Request):
        token = None
        if request.url.query:
            req_query = {q.split('=')[0]: q.split('=')[1] for q in request.url.query.split('&') if '=' in q}
            token = req_query.get('token')
        if token is None:
            try:
                token = ujson.loads(await request.body())['token']
            except Exception:
                pass
        if token is None:
                raise HTTPException(status_code=403, detail={
                    'success': 0,
                    'msg': '需要指定 token'
                })
        user = await AioUserSelector.from_token(token=token)
        if user is None:
            expired_user = await AioUserSelector.from_token(token=token, allow_expired=True)
            if expired_user is not None:
                raise HTTPException(status_code=401, detail={
                    'success': 0,
                    'msg': '该 token 已经过期了，请重新提供凭证'
                })
            raise HTTPException(status_code=403, detail={
                'success': 0,
                'msg': '根据 token 未找到用户'
            })
        if not user.active:
            raise HTTPException(status_code=401, detail={
                'success': 0,
                'msg': '您的账号为不活跃状态，无法访问集群服务'
            })
        if len(allowed_groups) > 0 and not user.in_any_group(allowed_groups):
            raise HTTPException(status_code=401, detail={
                'success': 0,
                'msg': '您所在的权限组不允许使用该接口'
            })
        if len(allowed_scopes) > 0 and user.access.access_scope not in allowed_scopes:
            raise HTTPException(status_code=401, detail={
                'success': 0,
                'msg': f'您当前的 access_scope({user.access.access_scope}) 不允许使用该接口'
            })
        for attr, value in kwargs.items():
            try:
                assert getattr(user, attr) == value
            except Exception:
                raise HTTPException(status_code=401, detail={
                    'success': 0,
                    'msg': '您无权访问该接口'
                })
        return user
    return __func


def get_internal_api_user_with_token(allowed_groups=[], allowed_scopes=[], **kwargs):
    return get_api_user_with_token(allowed_groups=[USER_ROLE.INTERNAL] + allowed_groups, allowed_scopes=allowed_scopes, **kwargs)


async def get_api_user_with_name(user_name: str = None):
    if user_name is None:
        raise HTTPException(status_code=401, detail={
            'success': 0,
            'msg': '非法请求'
        })
    user = await AioUserSelector.find_one(user_name=user_name)
    if user is None:
        raise HTTPException(status_code=401, detail={
            'success': 0,
            'msg': '根据user_name未找到用户'
        })
    return user


def check_user_access_to_task(task: TrainingTask, user: User):
    if task.task_type == TASK_TYPE.JUPYTER_TASK:
        if task.user_name != user.user_name and not user.in_any_group([JUPYTER_ADMIN_GROUP, TASK_ADMIN_GROUP]):
            raise HTTPException(status_code=403, detail='无权操作其他人的容器')
    elif task.user_name != user.user_name and not user.in_group(TASK_ADMIN_GROUP):
        msg = f'[未授权操作] [{user.user_name}] 不能操作 [{task.user_name}] 的任务[{task.job_info}]，已经阻断!'
        raise HTTPException(status_code=403, detail=msg)


def get_api_task(check_user=True, chain_task=True):
    async def __func(user: User = Depends(get_api_user_with_token()), chain_id: str = None, nb_name: str = None, id: int = None) -> TrainingTask:
        aio_selector = AioTrainingTaskSelector if chain_task else AioBaseTaskSelector
        if id is not None:
            t: TrainingTask = await aio_selector.find_one(None, id=id)
        elif chain_id is not None:
            t: TrainingTask = await aio_selector.find_one(None, chain_id=chain_id)
        else:
            t: TrainingTask = await aio_selector.find_one(None, nb_name=nb_name, user_name=user.user_name)
        if t is None:
            raise HTTPException(status_code=401, detail={
                'success': 0,
                'msg': f'no task of user [{user.user_name}] with id [{id}] or chain_id [{chain_id}] or nb_name [{nb_name}]!'
            })
        if check_user:
            check_user_access_to_task(task=t, user=user)
        if t.user_name == user.user_name:
            t.user = user
        return t
    return __func


async def get_new_nb_name(nb_name: str):
    return nb_name[len('DL_CLUSTER_'):]


class API_NOTES(BaseModel):
    content: str


class ChainIds(BaseModel):
    chain_id_list: str = ''
    saved_path: str = ''
