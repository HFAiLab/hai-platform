from datetime import datetime
from typing import Optional, Tuple

import munch
from cached_property import cached_property
from .mini_traits import MiniTraits as HasTraits
from .mini_traits import Int, NoneInt, Unicode, NoneStr, List, Dict, Any, Datetime, Bool
from .base_user import BaseUser

try:
    from k8s.v1_api import node_gpu_num
except:
    def node_gpu_num(*args, **kwargs): return 0


class BasePod:
    def __init__(self, task_id, pod_id, job_id, status, node, role, assigned_gpus=None, created_at=None, begin_at=None,
                 end_at=None, memory=0, cpu=0, exit_code='nan', **kwargs):
        """

        @param pod_id:
        @param status:
        @param node:
        @param job_id:
        @param xp_id:
        @param role:
        @param started_at:
        @param assigned_gpus:
        @param exit_code:
        """
        self.task_id = task_id
        self.pod_id = pod_id
        self.job_id = job_id
        self.status = status
        self.node = node
        self.role = role
        self.created_at = created_at
        self.begin_at = begin_at
        self.end_at = end_at
        self.memory = memory
        self.cpu = cpu
        self.exit_code = exit_code

        # 得暂时兼容老的 client
        self.started_at = self.begin_at
        self.job_uid = ''

        if assigned_gpus is None:
            assigned_gpus = list(range(node_gpu_num(node)))
        self.assigned_gpus = assigned_gpus

        if 'pod_xp_id' in kwargs:
            self.xp_id = kwargs['pod_xp_id']

        if 'pod_status' in kwargs:
            self.status = kwargs['pod_status']

    @property
    def cluster(self):
        return self.node[:2]

    @property
    def environments(self):
        return {
            'MARSV2_NODE_NAME': self.node,
            'MARSV2_RANK': self.job_id,
            'CONDARC': f'/marsv2/scripts/.condarc',
            # 暂时兼容老的 env
            'NODE_NAME': '${MARSV2_NODE_NAME}',
            'RANK': '${MARSV2_RANK}',
            'MARSV2_LOG_DIR': f'/marsv2/log/{self.task_id}',
            'MARSV2_LOG_FILE_PATH': '${MARSV2_LOG_DIR}/${MARSV2_NODE_NAME}#${MARSV2_RANK}',
            'MARSV2_DEBUG_LOG_FILE_PATH': '${MARSV2_LOG_DIR}/debug_${MARSV2_NODE_NAME}#${MARSV2_RANK}'
        }

    def __repr__(self):
        self_dict = self.__dict__
        return '\n'.join([f'{k}: {self_dict[k]}' for k in self_dict])


class BaseTask(HasTraits):
    """
    BaseTask 类，为集群启动任务的最小单位，目前子类包括训练任务、升级任务（之后可能有 kernel 任务）
    """
    id: int = NoneInt()
    nb_name: str = Unicode()
    user_name: str = Unicode()
    code_file: str = Unicode()
    workspace: str = Unicode()
    config_json: dict = Dict()
    group: str = Unicode()
    # 得暂时兼容老的 client，写成 any 吧
    nodes: int = Any()
    assigned_nodes: list = List()
    restart_count: int = Int()
    whole_life_state: int = Int()
    backend: str = Unicode()
    task_type: str = Unicode()
    queue_status: str = Unicode()
    notes: str = NoneStr()
    priority: int = Int()
    first_id: int = NoneInt()
    chain_id: str = NoneStr()
    stop_code: int = Int()
    suspend_code: int = Int()
    mount_code: int = Int()
    suspend_updated_at: datetime = Datetime()
    begin_at: datetime = Datetime()
    end_at: datetime = Datetime()
    created_at: datetime = Datetime()
    worker_status: str = Unicode(default_value='queued')
    star: bool = Bool()

    # additional
    scheduled_info: munch.Munch = Any()
    _pods_: list = List()
    tags: list = List()

    def __init__(self, implement_cls=None, **kwargs):
        super().__init__(**kwargs)
        self.parliament_attr = ()
        self.__implement_cls__ = implement_cls
        self.__impl__: Optional[ITaskImpl] = None
        self._user: Optional[BaseUser] = None


    def re_impl(self, implement_cls):
        self.__implement_cls__ = implement_cls
        self.__impl__: Optional[ITaskImpl] = None
        return self

    def _bind_impl_(func):
        def wrapper(self, *args, **kwargs):
            if self.__impl__ is None and self.__implement_cls__ is not None:
                self.__impl__ = self.__implement_cls__(self)
            return func(self, *args, **kwargs)
        return wrapper

    def _async_bind_impl_(coro_func):
        async def coro_wrapper(self, *args, **kwargs):
            if self.__impl__ is None and self.__implement_cls__ is not None:
                self.__impl__ = self.__implement_cls__(self)
            return await coro_func(self, *args, **kwargs)
        return coro_wrapper

    def set_scheduled_info(self, code, msg):
        self.scheduled_info = munch.munchify(dict(code=code, msg=msg))

    @cached_property
    def job_info(self):
        return f'[{self.user_name}][{self.nb_name}][{self.id}]'

    @_bind_impl_
    def create(self, *args, **kwargs):
        return self.__impl__.create(*args, **kwargs)

    @_bind_impl_
    def resume(self, *args, **kwargs):
        return self.__impl__.create(*args, **kwargs)

    @_bind_impl_
    def create_error_info(self, failed_msg):
        return self.__impl__.create_error_info(failed_msg)

    @_bind_impl_
    def update_config_json_by_path(self, path, value, *args, **kwargs):
        return self.__impl__.update_config_json_by_path(path, value, *args, **kwargs)

    @_async_bind_impl_
    async def aio_update_config_json_by_path(self, path, value, *args, **kwargs):
        return await self.__impl__.aio_update_config_json_by_path(path, value, *args, **kwargs)

    @_bind_impl_
    def update(self, fields: Tuple[str, ...], values: Tuple, *args, **kwargs):
        return self.__impl__.update(fields, values, *args, **kwargs)

    @_bind_impl_
    def tag_task(self, tag: str, *args, **kwargs):
        return self.__impl__.tag_task(tag, *args, **kwargs)

    @_bind_impl_
    def untag_task(self, tag: str, *args, **kwargs):
        return self.__impl__.untag_task(tag, *args, **kwargs)

    @_bind_impl_
    def star_task(self, star: bool, *args, **kwargs):
        return self.__impl__.star_task(star, *args, **kwargs)

    @_bind_impl_
    def unstar_task(self, star: bool, *args, **kwargs):
        return self.__impl__.unstar_task(star, *args, **kwargs)

    def re_pods(self):
        self.select_pods()
        return self

    async def aio_re_pods(self):
        await self.aio_select_pods()
        return self

    def append_pod(self, pod):
        self._pods_.append(pod)

    @_bind_impl_
    def update_pod_status(self, rank, status, *args, **kwargs):
        """更新pod status的方法"""
        self.__impl__.update_pod_status(rank, status, *args, **kwargs)

    @_bind_impl_
    def select_pods(self, *args, **kwargs):
        """添加pod的方法"""
        self.__impl__.select_pods(*args, **kwargs)

    @_async_bind_impl_
    async def aio_select_pods(self, *args, **kwargs):
        """添加pod的方法"""
        await self.__impl__.aio_select_pods(*args, **kwargs)

    def _select_pods_first_(func):
        def wrapper(self, *args, **kwargs):
            if len(self._pods_) == 0:
                self.select_pods()
            return func(self, *args, **kwargs)
        return wrapper

    @property
    @_select_pods_first_
    def pods(self):
        return self._pods_

    @property
    def cluster(self):
        return self.group.split(':')[-1].strip()[0:2]

    @cached_property
    def nodes_list(self):
        return [self.nodes]

    @cached_property
    def groups_list(self):
        return [g.split(':')[-1].strip() for g in self.group.split(';')]

    @_bind_impl_
    def _get_user(self):
        self._user = self.__impl__.user

    @property
    def user(self):
        if self._user is None:
            self._get_user()
        return self._user

    @user.setter
    def user(self, value):
        self._user = value

    @cached_property
    @_bind_impl_
    def environments(self):
        # 任务提交时自定义的 env
        return self.__impl__.environments

    @cached_property
    @_bind_impl_
    def sys_environments(self):
        # 系统预设的 env
        return self.__impl__.sys_environments

    @cached_property
    @_bind_impl_
    def train_environment(self):
        # 类似 ubuntu2004_cu113 等，指定的镜像、环境参数
        return self.__impl__.train_environment

    @cached_property
    @_bind_impl_
    def runtime_config_json(self) -> dict:
        return self.__impl__.get_runtime_config_json()

    @property
    @_async_bind_impl_
    async def aio_runtime_config_json(self):
        return await self.__impl__.aio_get_runtime_config_json()

    @_bind_impl_
    def build_schemas(self):
        return self.__impl__.build_schemas()

    @_bind_impl_
    def task_run_script(self, *args, **kwargs):
        """
        获取对应 rank 的 task_run 脚本：以用户身份去跑这个脚本，启动训练
        """
        return self.__impl__.task_run_script(*args, **kwargs)

    @_bind_impl_
    def set_restart_log(self, rule, reason, result, *args, **kwargs):
        return self.__impl__.set_restart_log(rule, reason, result, *args, **kwargs)

    @property
    def schema(self):
        """
        获取任务提交时候的 schema

        :return:
        """
        exp = self
        raw_task_schema = exp.config_json.get('schema', None)
        if raw_task_schema:
            return raw_task_schema
        code_file_with_param = exp.code_file.replace(exp.workspace, '')
        # | --- workspace --- | --- code_file --- |
        # | --- /a/b/     --- | --- /a/b/c.py --- |
        # | --- /a        --- | --- b/c.py    --- |
        if code_file_with_param.startswith('/'):
            code_file_with_param = code_file_with_param[1:]
        entrypoint = code_file_with_param.split(' ')[0]
        parameters = ' '.join(code_file_with_param.split(' ')[1:])
        environments = exp.config_json.get('environments', {})
        py_venv = None
        if 'HF_ENV_NAME' in environments:
            py_venv = environments['HF_ENV_NAME']
            if 'HF_ENV_OWNER' in environments:
                py_venv = f'{py_venv}[{environments["HF_ENV_OWNER"]}]'
        image = exp.backend
        if exp.config_json.get('train_image', None) is not None:
            image = exp.config_json.get('train_image', None)
            exp.backend = 'train_image:' + image.split('/')[-1]
        schema_dict = dict(
            version=2,
            name=self.nb_name,
            priority=self.config_json.get('priority', self.priority),
            spec=dict(
                workspace=self.workspace,
                entrypoint=entrypoint,
                parameters=parameters,
                environments=environments,
            ),
            resource=dict(
                image=image,
                group=exp.group,
                node_count=exp.nodes,
            ),
            options=dict(
                whole_life_state=exp.whole_life_state,
                mount_code=exp.mount_code,
            )
        )
        if py_venv:
            schema_dict['options']['py_venv'] = str(py_venv)
        return schema_dict


class ITaskImpl:
    def __init__(self, task: BaseTask):
        self.task = task

    def create(self, *args, **kwargs):
        raise NotImplementedError

    def resume(self, *args, **kwargs):
        raise NotImplementedError

    def create_error_info(self, failed_msg, *args, **kwargs):
        raise NotImplementedError

    def update_config_json_by_path(self, path, value, *args, **kwargs):
        raise NotImplementedError

    async def aio_update_config_json_by_path(self, path, value, *args, **kwargs):
        raise NotImplementedError

    def update(self, fields: Tuple[str], values: Tuple, *args, **kwargs):
        raise NotImplementedError

    def tag_task(self, tag: str, *args, **kwargs):
        return NotImplementedError

    def untag_task(self, tag: str, *args, **kwargs):
        return NotImplementedError

    def star_task(self, star: bool, *args, **kwargs):
        return NotImplementedError

    def unstar_task(self, star: bool, *args, **kwargs):
        return NotImplementedError

    def update_pod_status(self, rank, status, *args, **kwargs):
        raise NotImplementedError

    def select_pods(self, *args, **kwargs):
        raise NotImplementedError

    async def aio_select_pods(self, *args, **kwargs):
        raise NotImplementedError

    @cached_property
    def user(self, *args, **kwargs):
        raise NotImplementedError

    @cached_property
    def environments(self, *args, **kwargs):
        raise NotImplementedError

    @cached_property
    def sys_environments(self, *args, **kwargs):
        raise NotImplementedError

    @cached_property
    def train_environment(self, *args, **kwargs):
        raise NotImplementedError

    async def aio_get_runtime_config_json(self):
        raise NotImplementedError

    def get_runtime_config_json(self):
        raise NotImplementedError

    def build_schemas(self, *args, **kwargs):
        raise NotImplementedError

    def task_run_script(self, *args, **kwargs):
        raise NotImplementedError

    def set_restart_log(self, rule, reason, result, *args, **kwargs):
        raise NotImplementedError
