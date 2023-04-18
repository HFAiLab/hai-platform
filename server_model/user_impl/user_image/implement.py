
from .default import *
from .custom import *

from base_model.base_user_modules import IUserImage
from server_model.selector import AioTrainEnvironmentSelector


class UserImage(UserImageExtras, IUserImage):
    async def async_get_train_images(self):
        """ 获取内建的 train_images """
        mars_images = await AioTrainEnvironmentSelector.find_all()  # 萤火内建镜像
        await self.user.quota.create_quota_df()
        user_mars_images = []
        for mi in mars_images:
            if mi['env_name'] in self.user.quota.train_environments:
                mi['quota'] = int(self.user.quota.quota(f'train_environment:{mi["env_name"]}'))  # 这个是 np.int64
                user_mars_images.append(mi)
        return user_mars_images
