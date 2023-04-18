
from .default import *
from .custom import *

from abc import ABC
from cached_property import cached_property

from conf import CONF
from server_model.task_impl.single_task_impl import SingleTaskImpl


class ServiceTaskSchemaImpl(ServiceTaskSchemaImplExtras, SingleTaskImpl, ABC):

    @cached_property
    def train_environment(self, *args, **kwargs):
        env = super(ServiceTaskSchemaImpl, self).train_environment
        env.config['environments'] = env.config.get('environments', {})
        if not env.user_defined:
            env.config['environments']['USER_BIN_PYTHON'] = env.config.get('python', '/usr/bin/python3.6')
        is_spot = self.task.schema.get('resource', {}).get('is_spot', False)
        env.config['environments']['MARSV2_SPOT_JUPYTER'] = '1' if is_spot else '0'
        is_shared = self.task.group.startswith(CONF.jupyter.shared_node_group_prefix)
        env.config['environments']['MARSV2_SHARED_JUPYTER'] = '1' if is_shared else '0'
        restored_image = self.user.checkpoint.find_one(self.task.nb_name)
        if restored_image:  # 从 checkpoint 镜像中恢复时, 覆盖原有的 image 为 ckpt image
            env.image = restored_image['image_ref']
            env.user_defined = False
        return env

    def task_run_script(self, *args, **kwargs):
        return self.fix_script(f"""
        set +e
        {self.extra_run_scripts()}
        sleep infinity;
        """)

    def grant_user_group_script(self):
        script = super(ServiceTaskSchemaImpl, self).grant_user_group_script()
        # 这里同时把这个用户 shared_group 的人 user_name 和 user_id 注入容器
        for _, row in self.user.other_shared_group_users.iterrows():
            script += f"useradd --uid {row.user_id} --shell /usr/sbin/nologin --no-create-home {row.user_name}\n"
        script += '\n'
        return script
