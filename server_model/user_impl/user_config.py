

from base_model.base_user import BaseUser
from conf import CONF


class UserConfig:
    """
    用于获取和 User 有关的配置文件
    """
    def __init__(self, user: BaseUser):
        self.user = user

    def log_dir(self, *args, **kwargs):
        """
        用户存放日志的目录
        @return:
        """
        user = self.user
        return {d['role']: d['dir'] for d in CONF.experiment.log.dist}[user.role].replace('{user_name}', user.user_name)

    @property
    def max_ops(self):
        """
        最大的ops
        @return:
        """
        return CONF.experiment.log.max_ops

    @property
    def max_filesize(self):
        """
        最大的log日志文件大小
        @return:
        """
        return CONF.experiment.log.max_filesize

    @property
    def number_of_files(self):
        """
        存储的滚动日志文件个数
        @return:
        """
        return CONF.experiment.log.number_of_files

    @property
    def task_namespace(self) -> str:
        if self.user.role not in (config := CONF.launcher.task_namespaces_by_role):
            raise Exception(f'未配置 {self.user.role} 的 namespace')
        return config[self.user.role]
