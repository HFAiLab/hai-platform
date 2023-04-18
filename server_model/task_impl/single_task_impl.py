import json
import os
from abc import ABC
from typing import Optional, OrderedDict

from cached_property import cached_property
from munch import Munch

from api.task_schema import TaskSchema
from base_model.base_task import ITaskImpl
from conf import CONF, FileType
from conf.flags import TASK_TYPE, EXP_STATUS, QUE_STATUS
from db import redis_conn, MarsDB
from logm import logger
from .code import parse_code_cmd
from .runtime_mounts import add_runtime_mounts
from .runtime_envs import add_runtime_envs
from .runtime_sidecars import get_runtime_sidecars
from roman_parliament.attr_hooks import generate_parliament_attr_value
from server_model.pod import Pod
from server_model.selector import UserSelector
from server_model.selector.train_environment_selector import TrainEnvironmentSelector
from utils import convert_task_job_to_key


class SingleTaskImpl(ITaskImpl, ABC):
    def __init__(self, task):
        super().__init__(task)
        self._runtime_config_json = None

        self._runtime_mounts = []
        add_runtime_mounts(self)
        self._runtime_envs = {}
        add_runtime_envs(self)

    def select_pods(self):
        """
        处于挂起的任务，用之前任务的 pod，这样也能拿到日志

        @return:
        """
        self.task._pods_ = Pod.find_pods(self.task.id)

    @cached_property
    def user(self):
        return UserSelector.from_user_name(self.task.user_name)

    @cached_property
    def sys_environments(self, *args, **kwargs):
        return {
            'MARSV2_TASK_TYPE': self.task.task_type,
            'MARSV2_TASK_ID': str(self.task.id),
            'FULL_HFAI_COMMANDS': '1' if self.task.user.is_internal else '0',
            'MARSV2_TASK_ENTRYPOINT_EXECUTABLE': '1' if self.task.schema.get('spec', {}).get('entrypoint_executable', False) else '0',
            'MARSV2_TASK_BACKEND': self.task.backend,
            'MARSV2_NB_NAME': self.task.nb_name,
            'MARSV2_NB_GROUP': self.task.group,
            'WORLD_SIZE': str(self.task.nodes),
            'MARSV2_SERVER': 'http://' + os.environ['MARSV2_SERVER'],
            'MARSV2_VENV_PATH': f'/hf_shared/hfai_envs/{self.task.user_name}',  # 环境变量暂时保留
            'HAIENV_PATH': f'/hf_shared/hfai_envs/{self.task.user_name}',
            'MARSV2_BFF_URL': CONF.try_get(f'server_url.bff.{self.task.user.role}'),
        }

    @cached_property
    def environments(self, *args, **kwargs):
        return self.task.schema.get('spec', {}).get('environments', {})

    @cached_property
    def train_environment(self, *args, **kwargs) -> Optional[Munch]:
        try:
            assert len(self.user.quota.train_environments) > 0, f'用户[{self.user.user_name}]至少要有一个 train_environments'
            if self.task.backend.startswith('train_image:'):
                # 用户自定义镜像时, image URI 由 launcher 查数据后通过 env 指定给 manager, 此处无需处理
                return Munch(user_defined=True, image=os.environ.get('HFAI_IMAGE'), config={})
            assert self.task.backend in self.user.quota.train_environments, f'用户没有使用 {self.task.backend} 的权限'
            env = TrainEnvironmentSelector.find_one(self.task.backend)
            if env is None:  # fallback
                env = TrainEnvironmentSelector.find_one(self.user.quota.train_environments[0])
            return Munch(user_defined=False, **env)
        except Exception as e:
            logger.exception(e)
            return Munch(user_defined=False, image=None, config={})

    @staticmethod
    def fix_script(script):
        """
        用比较美观的方式去掉 python f-string 的前置空格，保持前后缩进关系
        """
        fixed_char = [' ', '\t']
        f = lambda s, n: n if s == '' or s[0] not in fixed_char else f(s[1:], n + 1)
        ss = [s for s in script.split('\n')]
        fixed_prefix = min(f(s, 0) for s in ss if len(s) and s[0] in fixed_char)
        return '\n'.join(s[fixed_prefix:] if len(s) and s[0] in fixed_char else s for s in ss) + '\n'

    def hf_envs_values(self, rank):
        __n = '\n'
        script = ''
        for env in [self.train_environment.config.get('environments', {}),
                    self.environments, self.sys_environments,
                    self.user.environment.environments, self.task.pods[rank].environments,
                    self._runtime_envs,
                    ]:
            script += self.fix_script(f"""
            {__n.join(f'export {k}={v}' for k, v in env.items())}
            """)
            script += '# -------------------------------------------------------'
        script += '\n'
        return script

    def haiprof_env_values(self):
        env = OrderedDict()
        profile = self.task.schema.get('options', {}).get('profile')
        if profile:
            env['HAIPROF_ENABLED'] = '0' if profile.get('follow_task') else '1'
            env['HAIPROF_TIME'] = profile.get('time', 0)
            env['HAIPROF_WARMUP'] = profile.get('warmup', 0)
            env['HAIPROF_TRACE'] = profile.get('trace', 0)
            env['HAIPROF_RECORDERS'] = profile.get('recorder', 'all')
            env['HAIPROF_LOG_DIR'] = profile.get('log_dir', '${MARSV2_LOG_DIR}/haiprof')
            env['HAIPROF_TASK_ID'] = profile.get('task_id', '${MARSV2_TASK_ID}')
            env['HAIPROF_USER_ID'] = profile.get('user_id', '${MARSV2_UID}')
            intervals = profile.get('interval', {})
            for k in intervals:
                env[f'HAIPROF_{k.upper()}_INTERVAL'] = intervals[k]
        else:
            env['HAIPROF_ENABLED'] = '0'

        script = '\n'.join(f'export {k}={v}' for k, v in env.items())
        script += '\n# ----------------------------------------------------\n'
        return script

    def grant_user_group_script(self):
        __n = '\n'
        script = ''
        script += self.fix_script(f"""
                    {__n.join(f'echo {ulg}:{self.user.user_name} | tee -a /etc/group > /dev/null' for ulg in self.user.quota.user_linux_group)}
                """)
        script += '\n'
        return script

    def task_run_script(self, code_dir, code_file, code_params, *args, **kwargs):
        # 顺便把不能启动中文任务的问题修复了
        task_schema: TaskSchema = TaskSchema.parse_obj(self.task.schema)
        if (py_venv := task_schema.options.get('py_venv', None)) is not None:  # like system[zwt];
            py_venv = str(py_venv)  # str it for 202111
            hf_env_name = py_venv.split('[')[0]
            hf_env_owner = py_venv.split('[')[-1].split(']')[0] if '[' in py_venv else ''
            # 为了兼容
            if hf_env_name in ['py3-202105', 'py3-202111', 'py38-202105', 'py38-202111', 'py38-202207']:
                hf_env_name = hf_env_name.split('-')[1]
                hf_env_owner = ''
            source_cmd = f'source haienv {hf_env_name}' + (f' -u {hf_env_owner}' if hf_env_owner else '')
            source_cmd = f'{source_cmd} || echo "no valid env found"'
        else:
            source_cmd = ''
        if str(watchdog_time := task_schema.options.get('watchdog_time', '')).isdigit():
            watchdog_time = int(watchdog_time)
            watchdog_time_cmd = \
                f'(python3 -c "from hfai.client.api import set_watchdog_time;set_watchdog_time({watchdog_time})" && ' \
                f'echo "set watchdog_time to {watchdog_time} succeeded") || echo "set watchdog_time to {watchdog_time} failed"'
        else:
            watchdog_time_cmd = ''
        if self.task.schema.get('spec', {}).get('entrypoint_executable', False):
            run_cmd = f'{code_file} {code_params}'
        else:
            if code_file.endswith('.py') or code_file.endswith(".py'"):
                profile = self.task.schema.get('options', {}).get('profile')
                if profile and profile.get('trace', 0) == 1:
                    run_cmd = f'haiprof trace {code_file} {code_params}'
                else:
                    run_cmd = f'python3 -u {code_file} {code_params}'
            else:  # .sh
                run_cmd = f'bash {code_file} {code_params}'
        __n = '\n'
        script = self.fix_script(f"""
        set -e
        ulimit -n 204800

        cd {code_dir}

        export PYTHONPATH=${{PWD}}:${{PYTHONPATH}}
        # user defined envs
        {__n.join(f'export {k}={v}' for k, v in self.environments.items())}

        {source_cmd}
        {watchdog_time_cmd}
        {run_cmd}
        """)
        return script

    def get_service(self, rank):
        # 创建各项服务所需的 k8s svc
        all_service = {'nodeports': [], 'headless_services': [], 'ingress_rules':[]}
        if rank == 0:
            services = self.task.schema.get('services', [])
            transform_service_name = lambda x : '' if x == 'jupyter' else f'/{x}'
            for service in services:
                if service['type'] == 'tcp':
                    all_service['nodeports'].append({'name': service['name'], 'port': service['port']})
                elif service['type'] == 'http':
                    all_service['headless_services'].append({'name': service['name'], 'port': service['port']})
                    all_service['ingress_rules'].append({
                        'path': f'/{self.task.user_name}/{self.task.nb_name}' + f'{transform_service_name(service["name"])}',
                        'port': service['port']
                    })
            # 需要创建至少一个 headless service 实现 DNS 解析
            if len(all_service['headless_services']) == 0:
                all_service['headless_services'] = [{'name': 'stub', 'port': 2222}]
        return all_service

    def get_pod_namespace(self):
        return CONF.launcher.task_namespace

    def get_pod_labels(self, rank):
        return {
            'task_id': str(self.task.id),
            'num': str(rank),
            'task_key': f'{convert_task_job_to_key(self.task, rank)}',
            'compute_node': 'true',
            'user_id': self.task.user_name,
            'task_type': self.task.task_type,
            'rank': str(rank),
            'user_role': self.task.user.role
        }

    def get_pod_caps(self):
        return self.user.quota.user_linux_capabilities

    def enable_privileged_pod(self):
        if self.task.user.is_internal and self.task.user.quota.quota('privileged'):
            return True
        return False

    def enable_pod_host_ipc(self):
        if self.task.user.is_internal and self.task.user.quota.quota('host_ipc'):
            return True
        return False

    def enable_pod_host_pid(self):
        if self.task.user.is_internal and self.task.user.quota.quota('host_pid'):
            return True
        return False

    def enable_pod_host_network(self):
        if self.task.user.is_internal and self.task.user.quota.quota('host_network'):
            return True
        return False

    def enable_share_process_namespace(self):
        return False

    def get_sidecars(self, rank, schema):
        return get_runtime_sidecars(self, rank=rank, schema=schema)

    def build_schemas(self, *args, **kwargs):
        """
        我们将 schema 拆分成

        source hf_env.values

        root_scope
            /usr/local/sbin/hf-system-scripts/0~49.sh or values
            bash system.sh
            /usr/local/sbin/hf-system-scripts/50~99.sh or values

        user_scope
            /usr/local/bin/hf-user-scripts/0~49.sh or values
            bash user.sh
            /usr/local/bin/hf-user-scripts/50~99.sh or values

        @param args:
        @param kwargs:
        @return:
        """
        train_environment = self.train_environment
        # 为每个节点创建 schema
        schemas = []
        mounts = self.user.storage.personal_storage(self.task)
        for runtime_mount in self._runtime_mounts:  # runtime_mounts 由 add_runtime_mounts 构建
            mounts.append(runtime_mount)

        code_dir, code_file, code_params = parse_code_cmd(self)
        # 添加代码路径
        self._runtime_envs.update({
            'MARSV2_TASK_WORKSPACE': code_dir,
            'MARSV2_TASK_ENTRYPOINT': os.path.join(code_dir, code_file),
        })
        for rank, pod in enumerate(self.task.pods):
            if self.task.nb_name == 't_unschedulable_ZpRm4tEQpY3XXHkA':
                cpu_requests = 100000
            elif os.environ.get('CI_TEST', '0') == '1':
                cpu_requests = 0
            elif os.environ.get('DEBUG', '0') == '1':
                cpu_requests = 0
            elif self.task.task_type != TASK_TYPE.TRAINING_TASK:
                cpu_requests = 0
            else:
                cpu_requests = pod.cpu
            schema = {
                'pod_id': pod.pod_id,
                'service': self.get_service(rank),
                'namespace': self.get_pod_namespace(),
                'labels': self.get_pod_labels(rank),
                'caps': self.get_pod_caps(),
                'privileged': self.enable_privileged_pod(),
                'host_ipc': self.enable_pod_host_ipc(),
                'host_pid': self.enable_pod_host_pid(),
                'host_network': self.enable_pod_host_network(),
                'share_process_namespace': self.enable_share_process_namespace(),
                'image': train_environment.image,
                'link_hfai_image': train_environment.user_defined,
                'mounts': mounts,
                'task_run_script': self.task_run_script(code_dir, code_file, code_params),
                'grant_user_group_script': self.grant_user_group_script(),
                'hf_envs_values': self.hf_envs_values(rank=rank),
                'haiprof_env_values': self.haiprof_env_values(),
                'sidecars': [],
                'node': pod.node,
                'node_selector': {
                    'kubernetes.io/hostname': pod.node
                },
                'resources': {
                    'cpu': {
                        'requests': cpu_requests,
                        'limits': pod.cpu,
                    },
                    'memory': {
                        'requests': 0,
                        'limits': pod.memory,
                    }
                }
            }
            # sidecar 有可能要改原有的 schema
            schemas.append(self.get_sidecars(rank=rank, schema=schema))
        return schemas

    def update_pod_status(self, rank, status, *args, **kwargs):
        if rank >= len(self.task.pods) and rank >= len(self.task.re_pods().pods):  # pod表还没更新完
            return
        if status == 'terminated':  # pod已经关完了
            if self.task.pods[rank].status in [EXP_STATUS.FAILED_TERMINATING, EXP_STATUS.STOPPED_TERMINATING, EXP_STATUS.SUCCEEDED_TERMINATING]:
                self.task.parliament_attr = generate_parliament_attr_value(exp=f'.pods[{rank}].status', value=self.task.pods[rank].status.split('_')[0])
            if all([pod.status in [EXP_STATUS.FAILED, EXP_STATUS.STOPPED, EXP_STATUS.SUCCEEDED] for pod in self.task.pods]) and self.task.queue_status != QUE_STATUS.FINISHED:  # 所有pod都已经结束
                redis_conn.lpush(f'{CONF.manager.stop_channel}:{self.task.id}', json.dumps({'action': 'stop_manager'}))
                redis_conn.expire(f'{CONF.manager.stop_channel}:{self.task.id}', 5 * 60)
        else:
            if status in [EXP_STATUS.FAILED, EXP_STATUS.STOPPED, EXP_STATUS.SUCCEEDED]:
                status += '_terminating'
            if self.task.pods[rank].status != status:
                self.task.parliament_attr = generate_parliament_attr_value(exp=f'.pods[{rank}].status', value=status)

    async def aio_get_runtime_config_json(self):
        if self._runtime_config_json is None:
            sql = f'''
                select coalesce(jsonb_object_agg("source", "config_json") filter ( where "source" is not null ), '{{}}'::jsonb) as "runtime_config_json"
                from task_runtime_config where "task_id" = {self.task.id}
            '''
            self._runtime_config_json = (await MarsDB().a_execute(sql)).fetchone()[0]
        return self._runtime_config_json

    def get_runtime_config_json(self):
        if self._runtime_config_json is None:
            sql = f'''
                select coalesce(jsonb_object_agg("source", "config_json") filter ( where "source" is not null ), '{{}}'::jsonb) as "runtime_config_json"
                from task_runtime_config where "task_id" = {self.task.id}
            '''
            self._runtime_config_json = MarsDB().execute(sql).fetchone()[0]
        return self._runtime_config_json
