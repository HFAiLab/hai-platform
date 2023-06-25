
import os
import rsa
import toml
import base64
import munch
import types


MANAGER_CONFIG_DIR = os.environ.get('MARSV2_MANAGER_CONFIG_DIR', '/etc/config')


def merge_conf(conf1: munch.Munch, conf2: munch.Munch):
    for k, v in conf2.items():
        if conf1.get(k):
            if isinstance(conf1[k], dict) and isinstance(conf2[k], dict):
                conf1[k] = merge_conf(conf1[k], conf2[k])
            else:
                conf1[k] = v
        else:
            conf1[k] = v
    return conf1


CONF = munch.Munch()


for file_name in ['core.toml', 'scheduler.toml', 'extension.toml', 'override.toml']:
    config_file = os.path.join(MANAGER_CONFIG_DIR, file_name)
    if os.path.exists(config_file):  # 若有这个文件，那么应该是合法的
        try:
            with open(config_file) as f:
                CONF = merge_conf(CONF, munch.Munch.fromDict(toml.loads(f.read())))
        except Exception as e:
            print(f'merge {file_name} 失败：{e}')

try:
    with open('/high-flyer/marsv2_private_key', 'r') as f:
        marsv2_private_key = f.read().encode()
except Exception:
    marsv2_private_key = None


def decrypt_message(target: str):
    try:
        if marsv2_private_key is not None:
            # 因为有时候不一定有这个 config
            exec(f"{target} = rsa.decrypt(base64.b16decode({target}.encode()), rsa.PrivateKey.load_pkcs1(marsv2_private_key)).decode()")
    except Exception:
        pass


decrypt_message("CONF.database.postgres.primary.password")
decrypt_message("CONF.database.postgres.secondary.password")
decrypt_message("CONF.database.redis.password")
decrypt_message("CONF.database.influxdb.password")
decrypt_message("CONF.database.fffs_influxdb.token")
decrypt_message("CONF.database.clickhouse.password")
decrypt_message("CONF.cloud.storage.access_key_id")
decrypt_message("CONF.cloud.storage.access_key_secret")
decrypt_message("CONF.cloud.storage.service.password")


def try_get(self, *args, default=None):
    """
    CONF.get('a', {}).get('b', {}).get('c', {})
    ---->
    CONF.try_get('a.b.c')
    CONF.try_get('a.b', 'c')
    CONF.try_get('a', 'b', 'c')
    CONF.try_get('a.b.c', default='hello')

    :param default:
    :param self:
    :param args:
    :return:
    """
    config = self
    args = sum([a.split('.') for a in args], [])
    for arg in args[:-1]:
        config = config.get(arg, {})
    return config.get(args[-1], default)


CONF.try_get = types.MethodType(try_get, CONF)
