import os
import sys
import munch

__config__ = None
__mars_url__, __mars_token__, __mars_bff_url__ = None, None, None


def _config_file():
    return os.path.expanduser(os.environ.get('HFAI_CLIENT_CONFIG', '~/.hfai/conf.yml'))


def save_config(conf):
    cf = _config_file()
    os.makedirs(os.path.dirname(cf), exist_ok=True)
    with open(cf, encoding='utf8', mode='w') as f:
        f.write(conf.toYAML())
    os.system(f'chmod 700 {cf}')
    print(f'初始化成功, 目标配置 {cf}, 配置如下: ')
    os.system(f'cat {cf}')


def read_config():
    global __config__
    if __config__:
        return __config__
    else:
        if not os.path.exists(_config_file()):
            return None
        else:
            try:
                __config__ = munch.Munch.fromYAML(open(os.path.expanduser(_config_file())))
                return __config__
            except Exception as e:
                print(f'配置文件异常{str(e)}，无法加载，请确保使用 hfai init 生成')
                sys.exit(1)


def get_config_var(config_key, env_name):
    conf = read_config()
    conf = conf.__dict__ if conf else {}
    if config_key in conf:
        return conf[config_key]
    if env_name in os.environ:
        return os.environ[env_name]
    print(f'在 {_config_file()} 中缺少[{config_key}] 这个配置项 或 系统中缺少 [{env_name}] 这个环境变量 ')
    sys.exit(1)


def get_mars_url():
    global __mars_url__
    if __mars_url__ is not None:
        return __mars_url__
    __mars_url__ = get_config_var('url', 'MARSV2_SERVER')
    return get_mars_url()


def get_mars_token():
    global __mars_token__
    if __mars_token__ is not None:
        return __mars_token__
    __mars_token__ = get_config_var('token', 'MARSV2_USER_TOKEN')
    return get_mars_token()


def get_mars_bff_url():
    global __mars_bff_url__
    if __mars_bff_url__ is not None:
        return __mars_bff_url__
    __mars_bff_url__ = get_config_var('bff_url', 'MARSV2_BFF_URL')
    return get_mars_bff_url()
