
from base_model.utils import setup_custom_finder

setup_custom_finder()

try:
    from conf.proj_conf import *
    from conf.cluster_info import *
    from conf.utils import *
except:
    from .proj_conf import *
    from .cluster_info import *
    from .utils import *
