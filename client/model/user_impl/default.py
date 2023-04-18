from hfai.base_model.base_user_modules import IUserImage
from ...api.api_config import get_mars_url as mars_url
from ...api.api_utils import async_requests, RequestMethod

class UserImage(IUserImage):
    async def async_get(self):
        url = f'{mars_url()}/ugc/user/train_image/list?token={self.user.token}'
        return await async_requests(RequestMethod.POST, url, retries=3, timeout=60)
