
import functools
import pickle
import time
from typing import Union

from db import redis_conn
from .table_config import spawn_private_tables
from .mq_utils import WatchThread
from .user_data import DBUserData, UserData
from .utils import log_info, is_roaming_enabled, log_debug


# Initializations
_user_data = None
_enable_roaming = None


def initialize_user_data_roaming(tables_to_subscribe=None, overwrite_enable_roaming=None):
    """
    初始化 UserData. 订阅表的列表可以不填或不填全, 使用到时会自动订阅.
    """
    global _user_data
    if _user_data is not None:
        log_debug(f'重复初始化, 忽略. (尝试订阅 {tables_to_subscribe}, 已订阅 {_user_data.subscribe_tables})')
        return

    global _enable_roaming
    _enable_roaming = overwrite_enable_roaming if overwrite_enable_roaming is not None else is_roaming_enabled()
    spawn_private_tables(is_roaming_enabled=_enable_roaming)

    if not _enable_roaming:
        log_info('初始化, 使用[DB]数据')
        _user_data = DBUserData()
        _user_data.subscribe_tables(tables_to_subscribe)
        return

    log_info('初始化, 使用[Redis缓存]数据')
    _user_data = UserData()
    WatchThread(user_data=_user_data).start()
    _user_data.subscribe_tables(tables_to_subscribe)
    _user_data.init_sync_point()


def get_user_data_instance() -> Union[UserData, DBUserData]:
    global _user_data
    if _user_data is None:
        initialize_user_data_roaming()
    return _user_data


def enabled_roaming() -> bool:
    if _enable_roaming is None:
        initialize_user_data_roaming()
    return _enable_roaming


# Decorators
def sync_from_db_afterwards(changed_tables):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            ret = fn(*args, **kwargs)
            get_user_data_instance().signal_sync_point(changed_tables=changed_tables.copy())
            return ret
        return wrapper
    return decorator

def async_sync_from_db_afterwards(changed_tables):
    def decorator(coro_fn):
        @functools.wraps(coro_fn)
        async def wrapper(*args, **kwargs):
            ret = await coro_fn(*args, **kwargs)
            get_user_data_instance().signal_sync_point(changed_tables=changed_tables.copy())
            return ret
        return wrapper
    return decorator


def update_user_last_activity(user_name: str):
    # 通过 redis 通知 user data 的 sync point 来修改
    redis_conn.lpush('user_data_last_activity_update', pickle.dumps({'user_name': user_name, 'ts': time.time()}))
