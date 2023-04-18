

from base_model.base_user import BaseUser
from conf import CONF


class UserEnvironment:
    def __init__(self, user: BaseUser):
        self.user = user

    @property
    def environments(self):
        res = {
            'MARSV2_UID': self.user.uid,
            'MARSV2_USER': self.user.user_name,
            'MARSV2_USER_ROLE': self.user.role,
            'MARSV2_USER_TOKEN': self.user.token,
            'HOME': f'/home/{self.user.user_name}',
            'NUMBER_OF_FILES': str(self.user.config.number_of_files),
            'MAX_OPS': self.user.config.max_ops,
            'MAX_FILESIZE': self.user.config.max_filesize,
            'PYTHONIOENCODING': 'utf-8',
            # 暂时兼容老的 env
            'HF_USER_TOKEN': '${MARSV2_USER_TOKEN}',
            'HF_USER': '${MARSV2_USER}',
            'HF_USER_GROUP': self.user.shared_group,
            'USER': '${MARSV2_USER}',
            # set local to utf8
            'LANG': 'en_US.UTF-8',
            'LC_ALL': 'C.UTF-8',
            'LC_CTYPE': 'C.UTF-8',
        }
        res.update(CONF.experiment.get('user_env', {}).get(self.user.role, {}))
        return res
