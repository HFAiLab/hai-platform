
from hfai.base_model.utils import setup_custom_finder
setup_custom_finder()

from .api import set_watchdog_time
from .api import set_whole_life_state, get_whole_life_state
from .api import receive_suspend_command, go_suspend
from .api import EXP_PRIORITY, set_priority, WARN_TYPE
from .api import get_experiment, get_experiments
from .api import create_experiment
from .api import disable_warn
from . import remote
try:
    from .api.custom import *
except ImportError:
    pass
