# 集群相关的配置信息，和用户是不是 debug 有关
import os
import getpass

try:
    # for server
    from conf import CONF
    prefix = f'DEBUG_{getpass.getuser()}_' if os.environ.get('DEBUG', '0') == '1' else ''
except:
    # for client, 这个之后应该没有用了
    prefix = f'DEBUG_{getpass.getuser()}_' if os.environ.get('DEBUG', '0') == '1' else ''

forced_prefix = os.environ.get('MARS_PREFIX', None)
if forced_prefix is not None:
    mars_group_prefix = f"{forced_prefix}_"
else:
    mars_group_prefix = prefix

# 标记集群的版本
MARS_GROUP_FLAG = f'{mars_group_prefix}mars_group'

if os.environ.get('CI_TEST', '0') == '1':  # 跑CI或是DEBUG=0的情况下container_name都是hf-experiment
    CONTAINER_NAME = 'hf-experiment'
else:
    CONTAINER_NAME = f'{prefix.lower().replace("_", "-")}hf-experiment'

CONTAINER_NAME = 'debug-hf-experiment' if CONTAINER_NAME != 'hf-experiment' else 'hf-experiment'
