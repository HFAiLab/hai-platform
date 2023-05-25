
import functools
import multiprocessing
import os
import pathlib
import time

from conf import CONF
from logm import logger

global process_name
process_name = None


def set_process_name():
    global process_name
    if process_name is None:
        process_name = multiprocessing.current_process().name


def log_with_process_name(log_lv: str, log_str):
    set_process_name()
    logger.log(log_lv, f'{process_name} | [UserData] {log_str}')


def log_debug(log_str):
    log_with_process_name('DEBUG', log_str)


def log_warning(log_str):
    log_with_process_name('WARNING', log_str)


def log_info(log_str):
    log_with_process_name('INFO', log_str)


last_fetion_time = dict()

def log_error(log_str, exception=None, fetion_interval=None):
    log_with_process_name('ERROR', log_str)
    if isinstance(fetion_interval, int) and time.time() - last_fetion_time.get(log_str, 0) > fetion_interval:
        last_fetion_time[log_str] = time.time()
        logger.f_error(log_str)
    if exception is not None:
        logger.exception(exception)


def acquired_lock_file(path):
    try:
        pathlib.Path(path).touch(exist_ok=False)
        return True
    except:
        return False


module_name = os.environ.get('MODULE_NAME', '')
def is_roaming_enabled():
    return module_name in set(CONF.parliament.senator_list) - {'scheduler'} | {'manager'}


def is_sync_point():
    return os.environ.get('USER_DATA_SYNC_POINT', '0') == '1'


def sync_point_only(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if is_sync_point():
            return func(*args, **kwargs)
    return wrapper
