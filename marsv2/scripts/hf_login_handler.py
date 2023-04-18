

import os
import uuid
import requests

from tornado.escape import url_escape
from jupyter_server.auth.login import LoginHandler

try:
    from hfai.client.api.api_config import get_mars_url
    mars_url = get_mars_url()
except:
    # 兼容hai环境
    mars_url = os.environ['MARSV2_SERVER']

studio_url = os.environ['MARSV2_STUDIO_ADDR']

class HFLoginHandler(LoginHandler):
    allowed_tokens = set()
    not_allowed_tokens = set()

    @classmethod
    def get_user_token(cls, handler):
        """
        目前仅支持 url?token=xxx 这样的登录方式，页面输入密码是另一套，直接跳转 studio
        """
        user_token = cls.get_token(handler)
        if user_token not in cls.allowed_tokens:
            # 不存在一个 token 突然可以访问了这种情况，直接拉黑减轻 server 压力
            if not user_token or user_token in cls.not_allowed_tokens:
                return None
            token = handler.token
            if not token:
                return
            cls.allowed_tokens.add(token)
            # 说明这个 user_token 需要向 server 请求验证
            try:
                user_info = requests.post(f'{mars_url}/query/user/info?token={user_token}').json()['result']
                assert user_info['access_scope'] == 'all'
                assert user_info['user_name'] == os.environ['MARSV2_USER']
                cls.allowed_tokens.add(user_token)
            except:  # 这里不能只 catch Exception，因为 get_mars_url 有可能直接 exit(1)
                cls.not_allowed_tokens.add(user_token)
                return None
        authenticated = False
        if user_token in cls.allowed_tokens:
            handler.log.debug("Accepting token-authenticated connection from %s", handler.request.remote_ip)
            authenticated = True
        if authenticated:
            return uuid.uuid4().hex
        else:
            return None

    def _render(self, message=None):
        # 登录页面直接重定向到 studio
        next_params = url_escape(f'{self.request.full_url()}')
        self.redirect(f'https://{studio_url}?next={next_params}')
