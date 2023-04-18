import asyncclick as click
import munch
import os

from .utils import HandleHfaiCommandArgs, is_hai
from hfai.client.api.api_config import save_config
from hfai.client.api.api_utils import request_url, RequestMethod


envs_config_ready = os.environ.get('MARSV2_BFF_URL') and os.environ.get('MARSV2_SERVER')

class InitHandleHfaiCommandArgs(HandleHfaiCommandArgs):
    def format_usage(self, ctx, formatter):
        pieces = self.collect_usage_pieces(ctx)
        pieces = [f'<{p}>' for p in pieces[1:]] + ([] if envs_config_ready else ['[OPTIONS]'])
        formatter.write_usage(ctx.command_path, " ".join(pieces))

    def format_options(self, ctx, formatter):
        with formatter.section("Arguments"):
            formatter.write_dl(rows=[('token', '用于认证 hfai 服务的密钥，由管理员分发')])
        if not envs_config_ready:
            opts = []
            for param in self.get_params(ctx):
                rv = param.get_help_record(ctx)
                if rv is None:
                    continue
                if any([n in param.name for n in ['bff_url']]) and is_hai:
                    continue
                opts.append(rv)

            with formatter.section("Cluster Options"):
                formatter.write_dl(opts)
        else:
            # 有这些环境变量的都不提示这些了，用户不需要填的
            pass


@click.command(cls=InitHandleHfaiCommandArgs)
@click.argument('token', metavar='token')
@click.option('--url',   # hai 必须自己指定；在萤火上读环境变量；萤火外，会默认成 bff.xxx
              required=True,
              default=None if is_hai else os.environ.get('MARSV2_SERVER', 'http://api.yinghuo.high-flyer.cn'),
              show_default=True,
              help='初始化时，用户指定萤火二号的server，如果没特别需求不用填')
@click.option('--bff_url', # hai 不需要；在萤火上读环境变量；萤火外，会默认成 bff.xxx
              required=not is_hai,
              default=None if is_hai else os.environ.get('MARSV2_BFF_URL', 'https://bff.yinghuo.high-flyer.cn/proxy/s'),
              show_default=True,
              help='中转BFF url，如果没特别需求不用填。在萤火上会自动读环境变量，不用填；在萤火外，需要自己指定成绝对路径')
def init(token, url, bff_url):
    """ 
    初始化用户账户
    """
    if not (url.startswith('http://') or url.startswith('https://')):
        url = 'https://' + url

    if not token.startswith('ACCESS-'):
        try:
            print('发现原始 token，向 server 端申请注册 access token')
            query_url = f'{url}/operating/user/access_token/create?token={token}'
            token = request_url(RequestMethod.POST, query_url)['result']['access_token']
        except:
            print('向 server 端申请注册 access token 失败，保存原始 token')
    conf = munch.Munch.fromDict({
        'token': token,
        'url': url
    })
    if bff_url is not None:
        conf.bff_url = bff_url

    save_config(conf)
