
import multiprocessing
import os
import pickle
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from typing import Dict, Optional, Type, Union

import ujson

from conf import CONF
from db import redis_conn
from .data_table import IDataTable, InMemoryTable
from .table_config import TABLES, UserTable
from .mq_utils import MessageQueue, MessageType
from .utils import log_debug, log_info, log_error, log_warning, acquired_lock_file, sync_point_only


class UserDataBase(object):
    def __init__(self):
        self._subscribed_tables = []

    def _get_df(self, table_name):
        raise NotImplementedError

    async def _async_get_df(self, table_name):
        raise NotImplementedError

    def _subscribe_if_not(self, table_name):
        if table_name in self._subscribed_tables:
            return
        if table_name in TABLES.keys():
            log_debug(f'尝试获取未订阅的表 {table_name}, 进行订阅, 如果正在处理请求 可能影响响应速度.')
            self.subscribe_tables([table_name])
        else:
            raise AttributeError(f'Unrecognized Dataframe reqeust: {table_name}')

    def get_df(self, table_name):
        self._subscribe_if_not(table_name)
        return self._get_df(table_name)

    def async_get_df(self, table_name):
        self._subscribe_if_not(table_name)
        return self._async_get_df(table_name)

    def _subscribe_single_table(self, table_name):
        if table_name not in self._subscribed_tables:
            self._subscribed_tables.append(table_name)
        return True

    def subscribe_tables(self, table_names: Optional[Union[str, list]]):
        table_names = list(TABLES.keys()) if table_names == '*' else (table_names or [])
        for table_name in table_names:
            assert table_name in TABLES.keys(), f'试图订阅不存在的 table: {table_name}'
            table = TABLES[table_name]
            if not table.is_computed:
                success = self._subscribe_single_table(table_name)
            else:
                # 订阅 Computed Table, 必须同时订阅所有的 dependency 表
                success = \
                    all(self._subscribe_single_table(dependant.table_name) for dependant in table.dependencies) \
                    and self._subscribe_single_table(table_name)
            if not success:
                log_error(f'订阅表 [{table_name}] 失败')

    def sync_from_db(self, tables_to_sync=None):
        pass

    async def async_sync_from_db(self, tables_to_sync=None):
        pass

    def signal_sync_point(self, changed_tables=None):
        pass

    def signal_reload(self, msg):
        pass


class DBUserData(UserDataBase):
    def _get_df(self, table_name):
        TABLES[table_name].before_get_df_hook()
        return TABLES[table_name].get_df()

    async def _async_get_df(self, table_name):
        await TABLES[table_name].async_before_get_df_hook()
        return TABLES[table_name].get_df()

    def _subscribe_single_table(self, table_name):
        if issubclass(TABLES[table_name], InMemoryTable):
            raise Exception(f'当前进程中 UserData 不接入议会, 无法读写纯议会内存表 [{table_name}]')
        return super()._subscribe_single_table(table_name)


class UserData(UserDataBase):
    def __init__(self):
        self._tables: Dict[str, Type[IDataTable]] = {}
        super(UserData, self).__init__()
        # Initialize sync point
        self._name = os.environ.get('POD_NAME', 'unknown_pod') + '_' + multiprocessing.current_process().name
        # 每个 svc pod 只需要一个进程回复数据请求
        self.is_manager = os.environ.get('MODULE_NAME') == 'manager'
        self.is_pod_master = not self.is_manager and acquired_lock_file('/tmp/user_data_pod_master.lock')

    @sync_point_only
    def init_sync_point(self):
        log_info('初始化 Sync point')
        self._sync_thread = ThreadPoolExecutor(max_workers=1)   # 串行执行 sync 操作
        self._last_sync_signal_ts = defaultdict(lambda: 0)
        self._throttling_cnt = defaultdict(lambda : 0)
        self._max_throttling_time = CONF.user_data_roaming.get('max_num_throttling', 5)
        self._pending_reload = None
        while len(self._tables) != len(TABLES):
            log_warning('未成功订阅所有表, 阻塞等待并重试订阅.')     # 可能 DB 还没起来, sync point 必须订阅全部的表才能启动
            self.subscribe_tables(list(TABLES.keys()))
            time.sleep(1)
        self.user_last_activity_in_ns = {user: time.time_ns() for user in UserTable.df.user_name.tolist()}
        Thread(target=self._sync_timer, daemon=True).start()
        Thread(target=self._redis_dumper, daemon=True).start()
        Thread(target=self._last_activity_modifier, daemon=True).start()
        self.signal_reload("SyncPoint 重启")

    def _sync_timer(self):
        while True:
            time.sleep(CONF.user_data_roaming.get('sync_interval', 1.0))
            try:
                self.sync_from_db()
                if self._pending_reload is not None:
                    self.signal_reload(self._pending_reload)
                    self._pending_reload = None
            except Exception as e:  # 兜底, 这个线程不能挂, 否则议会可能丢数据
                log_error('Sync from db 出错', e, fetion_interval=60)
                time.sleep(1)

    def _last_activity_modifier(self):
        while True:
            try:
                data = redis_conn.brpop('user_data_last_activity_update')
                data = pickle.loads(data[1])
                self.user_last_activity_in_ns[data['user_name']] = time.time_ns()
            except Exception as e:
                log_error(f'处理 last activity 更新请求失败  {e}', e, fetion_interval=60)
                # 可能是 redis down, 会丢消息, 主动刷新全部用户的时间戳
                self.user_last_activity_in_ns = {k: time.time_ns() for k in self.user_last_activity_in_ns}
                time.sleep(1)

    def _redis_dumper(self):
        """ 仅在 sync point 进程中开线程执行 """
        while True:
            time.sleep(1)
            # 将最近有活动的用户 dump 到 redis 中
            try:
                result = [
                    {'ts': int(time_ns), 'user_name': user_name}
                    for user_name, time_ns in self.user_last_activity_in_ns.items() if isinstance(user_name, str)
                ]
                result.sort(key=lambda x: x['ts'], reverse=True)
                redis_conn.set('user_last_activity_in_ns', ujson.dumps(result))
                # 主动更新一下 computed tables, 以便内容有更新时触发其注册的 update_hook, 目前用于 `UserWithAllGroupsTable` 的 update 时间戳更新
                for table in self._tables.values():
                    if table.is_computed:
                        self._get_df(table.table_name)
            except Exception as e:
                log_error(f'dump user last activity failed: {e}', exception=e, fetion_interval=60)

    def _get_df(self, table_name):
        self._tables[table_name].before_get_df_hook()
        return self._tables[table_name].get_df()

    async def _async_get_df(self, table_name):
        await self._tables[table_name].async_before_get_df_hook()
        return self._tables[table_name].get_df()

    def _make_patches(self, table_names: set) -> list:
        patches = []
        for table_name in table_names:
            self._throttling_cnt[table_name] = 0
            if (patch := self._tables[table_name].pull_diff_from_db()) is not None:
                patches.append(patch)
        return patches

    def _sync_from_db(self, signal_ts, tables_to_sync=None):
        throttled_tables = set(
            table for table in tables_to_sync
            if self._last_sync_signal_ts[table] != signal_ts and self._throttling_cnt[table] < self._max_throttling_time
        )
        if len(tables_to_sync:= set(tables_to_sync) - throttled_tables):
            patches = self._make_patches(table_names=tables_to_sync)
            if len(patches) > 0:
                log_info(f'从 DB 同步表 {tables_to_sync}, 有差异的表: {[p["table_name"] for p in patches]}')
            else:
                log_debug(f'从 DB 同步表 {tables_to_sync}, 无变化')
            self.patch(patches, broadcast=True)
        if len(throttled_tables) > 0:
            log_debug(f'Sync signals are throttled for table: {throttled_tables}')
            for table in throttled_tables:
                self._throttling_cnt[table] += 1

    def _expand_table_list(self, tables: Optional[list]) -> list:
        table_set = set(tables)
        for table in self._tables.values():
            dependency_set = set(x.table_name for x in table.dependencies)
            if dependency_set & table_set:
                table_set.add(table.table_name)
        return list(table_set)

    def respond_in_mem_table_request(self, table_name, key):
        table: Type[InMemoryTable] = TABLES[table_name] # noqa
        if not self.is_pod_master or table_name not in self._tables or not table.initialized():
            # 自己的表还没初始化好或者不负责 respond, 不处理请求
            return
        redis_conn.set(key, self._name, nx=True, ex=60)
        if (value := redis_conn.get(key)) is not None and value.decode() == self._name:   # 抢到锁了, dump 数据
            payload = {'data': table.get_df(), 'birth_time': table.data_birth_time, 'timestamp': table.timestamp, 'from': self._name}
            redis_conn.lpush(f'{key}_response', pickle.dumps(payload))
            redis_conn.expire(f'{key}_response', 60)

    def _subscribe_single_table(self, table_name):
        if table_name not in self._tables:
            TABLES[table_name].initialize()
            if not TABLES[table_name].initialized():
                return False
            self._subscribed_tables.append(table_name)
            self._tables[table_name] = TABLES[table_name]
        return True

    def patch(self, patches: list, broadcast=True):
        if len(patches) > 0:
            for patch in patches:
                if TABLES[patch["table_name"]].receiving_patch:
                    TABLES[patch["table_name"]].patch(patch)
            # 通过议会广播变动
            if broadcast:
                try:
                    MessageQueue.send(MessageType.PATCH, patches)
                except Exception as e:
                    log_error('发送 patch 消息失败', e)
                    self._pending_reload = '有 patch 消息发送失败'

    def reload_dfs(self, table_names=None):
        table_names = self._subscribed_tables if table_names is None else table_names
        for table_name in table_names:
            self._tables[table_name].reload()

    def _sync_callback(self, worker):
        if worker.exception():
            log_error('Sync from db 出错: 执行 sync 出错', worker.exception())

    @sync_point_only
    def sync_from_db(self, tables_to_sync=None):
        tables_to_sync = self._expand_table_list(tables_to_sync) if tables_to_sync is not None else self._subscribed_tables
        ts = int(time.time() * 10000)
        for table in tables_to_sync:
            self._last_sync_signal_ts[table] = ts
        # throttling_time 时间内没有其他 sync signal 则处理本次 signal, 否则处理更新的 sync signal
        threading.Timer(
            CONF.user_data_roaming.get('sync_throttling_time', 0.1),
            lambda: self._sync_thread.submit(self._sync_from_db, signal_ts=ts,
                                             tables_to_sync=tables_to_sync).add_done_callback(self._sync_callback)
        ).start()

    def signal_sync_point(self, changed_tables=None):
        MessageQueue.send(MessageType.SYNC, changed_tables)

    def signal_reload(self, msg):
        MessageQueue.send(MessageType.RELOAD, msg)
