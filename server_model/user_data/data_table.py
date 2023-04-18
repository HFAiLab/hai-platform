
from __future__ import annotations

import asyncio
import os
import pickle
import threading
import time
import uuid
from datetime import datetime
from itertools import chain
from threading import Lock
from typing import Optional, List, Iterable

import pandas as pd

from conf import CONF
from db import redis_conn
from .mq_utils import MessageQueue, MessageType
from .patchable_dataframe import PatchableDataFrame, PatchConflictException
from .utils import log_debug, log_info, log_error


class IDataTable:
    """
    接口类, 不可被直接继承
    """
    table_name = None
    columns = None
    primary_key_columns = None
    receiving_patch = False
    dependencies: Optional[List[IDataTable]] = None
    is_computed = False
    lock: Optional[Lock] = None
    timestamp = None

    def __init_subclass__(cls, table_name=None, columns=None, dependencies=None, **kwargs):
        cls.table_name = table_name
        cls.columns = columns
        cls.dependencies = [] if dependencies is None else dependencies
        cls.lock = Lock()
        cls.timestamp = time.time()

    @classmethod
    def get_df_no_copy(cls):
        raise NotImplementedError

    @classmethod
    def before_get_df_hook(cls):
        raise NotImplementedError

    @classmethod
    async def async_before_get_df_hook(cls):
        cls.before_get_df_hook()

    @classmethod
    def get_df(cls) -> pd.DataFrame:
        return cls.get_df_no_copy().copy()

    @classmethod
    def reload(cls):
        raise NotImplementedError

    @classmethod
    async def async_reload(cls):
        cls.reload()

    @classmethod
    def initialize(cls):
        cls.reload()

    @classmethod
    def patch(cls, patch: dict):
        pass

    @classmethod
    def pull_diff_from_db(cls):
        raise NotImplementedError

    @classmethod
    def update_hook(cls):
        pass

    @classmethod
    def update_timestamp(cls, timestamp=None):
        cls.timestamp = timestamp or time.time()
        cls.update_hook()

    @classmethod
    def initialized(cls):
        raise NotImplementedError


class RoamingSqlTable(IDataTable):
    """
    使用议会时, 基于 SQL 的表, 继承此类后重写 sql() 方法
    """
    _df: Optional[PatchableDataFrame] = None
    _patch_buffer = None
    _patch_buffer_lock = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._df = None
        cls._patch_buffer = []
        cls._patch_buffer_lock = threading.Lock()

    @classmethod
    def get_df_no_copy(cls):
        return cls._df.df

    @classmethod
    def sql(cls):
        raise NotImplementedError

    @classmethod
    def _replace_with_newer_df(cls, new_df):
        with cls.lock:
            if cls._df is None or new_df.timestamp > cls._df.timestamp:
                cls._df = new_df
        cls.update_timestamp()

    @classmethod
    def _apply_patch(cls, patch: dict):
        (to_del, to_add) = patch['diff']
        with cls.lock:
            if cls._df.timestamp >= patch['timestamp']:
                log_info(f'忽略过期的 patch. table [{cls.table_name}] 时间差 {cls._df.timestamp - patch["timestamp"]}')
                return
            try:
                cls._df.apply_patch(to_del, to_add)
            except PatchConflictException as e:
                e.table_name = cls.table_name
                raise e
        cls.update_timestamp()
        log_debug(f'应用 patch 成功: table [{cls.table_name}]')

    @classmethod
    def _apply_patches_in_buffer(cls):
        if len(cls._patch_buffer) == 0:
            return
        with cls._patch_buffer_lock:
            cls._patch_buffer.sort(key=lambda patch:patch['timestamp'])
            patch_buffer_to_process = cls._patch_buffer
            cls._patch_buffer = []
        for patch in patch_buffer_to_process:
            cls._apply_patch(patch)

    @classmethod
    def _apply_patches_on_err_reload(cls):
        try:
            cls._apply_patches_in_buffer()
        except PatchConflictException as e:
            log_info(f'{e.table_name} 表 apply patch 时有冲突, 从 DB 中重新加载 df ({e})')
            cls.reload()
        except Exception as e:
            log_error(f'apply Patch 时发生错误! 重新加载 df', e)
            cls.reload()

    @classmethod
    async def _async_apply_patches_on_err_reload(cls):
        try:
            cls._apply_patches_in_buffer()
        except PatchConflictException as e:
            log_info(f'{e.table_name} 表 apply patch 错误, 从 DB 中重新加载 df ({e})')
            await cls.async_reload()

    @classmethod
    def patch(cls, patch: dict):
        """
        应用数据变动的 patch. 会在议会线程中调用, 需保证线程安全.
        Patch 不会立即生效，而是加入 buffer 中:
         1. 每次打 patch 时, 若 buffer 中的 patch age 超过一定阈值则立刻应用 buffer 中的所有 patch.
            这样短时间内连续接收到的 patch 会被缓存, 由于应用 patch 时会按时间戳排序, 可以避免因为网络问题导致 patch 到达顺序错乱的问题.
         2. 访问 df、计算 diff 时需要最新的数据，因此会立刻应用 buffer 中的所有 patch. 极端场景下会导致数据错乱, 但会触发 reload 兜底.
        """
        assert patch['table_name'] == cls.table_name, f'Wrong Table: patching [{cls.table_name}] {patch}'
        if len(cls._patch_buffer) > 0 and time.time() - cls._patch_buffer[-1]['timestamp'] > CONF.user_data_roaming.patch_ttl:
            log_debug(f'{cls.table_name} 表 buffer 中的 patch 已过缓存时间, 开始应用 buffer 中的 patches')
            cls._apply_patches_on_err_reload()
        with cls._patch_buffer_lock:
            cls._patch_buffer.append(patch)

    @classmethod
    def before_get_df_hook(cls):
        cls._apply_patches_on_err_reload()

    @classmethod
    async def async_before_get_df_hook(cls):
        await cls._async_apply_patches_on_err_reload()

    @classmethod
    def pull_diff_from_db(cls):
        cls._apply_patches_on_err_reload()
        db_df = PatchableDataFrame.load_from_db(sql=cls.sql())
        df_diff = cls._df.diff(db_df)
        return {'table_name': cls.table_name, 'diff': df_diff, 'timestamp': db_df.timestamp} if df_diff is not None else None

    @classmethod
    def reload(cls):
        try:
            log_debug(f'Reload table [{cls.table_name}]')
            new_df = PatchableDataFrame.load_from_db(sql=cls.sql())
            cls._replace_with_newer_df(new_df)
        except Exception as e:
            log_error(f'Reload table {cls.table_name} failed!', e)

    @classmethod
    async def async_reload(cls):
        try:
            log_debug(f'Async Reload table [{cls.table_name}]')
            new_df = await PatchableDataFrame.async_load_from_db(sql=cls.sql(), columns=cls.columns)
            cls._replace_with_newer_df(new_df)
        except Exception as e:
            log_error(f'Async Reload table {cls.table_name} failed!', e)

    @classmethod
    def initialize(cls):
        cls.reload()
        cls.receiving_patch = True

    @classmethod
    def initialized(cls):
        return cls._df is not None


class DBSqlTable(IDataTable):
    """
    完全使用 DB 数据时, 基于 SQL 的表, 继承此类后重写 sql() 方法
    """
    _df : Optional[pd.DataFrame] = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

    @classmethod
    def get_df_no_copy(cls):
        return cls._df

    @classmethod
    def sql(cls):
        raise NotImplementedError

    @classmethod
    def reload(cls):
        pass        # DB 表无需 reload, 每次都会加载最新的

    @classmethod
    def pull_diff_from_db(cls):
        pass        # DB 表不同步 diff

    @classmethod
    def _is_df_changed(cls, current_df):
        """
        注意此处是简单逐行比较的，DB 数据没变化不一定返回 False，因为没有 Order by 时 select 不保证顺序稳定.
        但数据变化时一定会返回 True, 不会导致 ComputedTable 更新滞后.
        """
        if len(cls._df) != len(current_df):
            return True
        return (cls._df.fillna(value=0) != current_df.fillna(value=0)).any().any()

    @classmethod
    def _update_df_if_changed(cls, new_df):
        if cls._df is None or cls._is_df_changed(current_df=new_df):
            cls._df = new_df
            cls.update_timestamp()
            log_debug(f'更新了 DB Table [{cls.table_name}]')

    @classmethod
    def before_get_df_hook(cls):
        new_df = PatchableDataFrame.load_from_db(sql=cls.sql(), get_raw_df=True).drop(['query_timestamp'], axis='columns')
        cls._update_df_if_changed(new_df)

    @classmethod
    async def async_before_get_df_hook(cls):
        new_df = await PatchableDataFrame.async_load_from_db(cls.sql(), columns=cls.columns, get_raw_df=True)
        new_df = new_df.drop(['query_timestamp'], axis='columns')
        cls._update_df_if_changed(new_df)

    @classmethod
    def initialized(cls):
        return True # 无需提前加载


class BaseTable:
    """
    基础 SQL 表的查询语句格式固定，可以继承 basetable 系列类后设置 columns，不必重写 sql 方法
    """
    @classmethod
    def sql(cls):
        return f'''
            select {', '.join([f'"{column}"' for column in cls.columns])}, current_timestamp as "query_timestamp"
            from "{cls.table_name}" order by 1
        '''

class DBBaseTable(BaseTable, DBSqlTable):
    pass


class RoamingBaseTable(BaseTable, RoamingSqlTable):
    pass


class ComputedTable(IDataTable):
    """
    本地计算得到的表, 继承此类后重写 compute() 方法
    """
    _df: Optional[pd.DataFrame] = None
    is_computed = True

    def __init_subclass__(cls, dependencies=None, **kwargs):
        dependencies = list(set(cls.collect(dependencies))) if dependencies is not None else []
        super().__init_subclass__(dependencies=dependencies, **kwargs)

    @classmethod
    def collect(cls, tables: List[IDataTable]) -> Iterable[IDataTable]:
        """ 递归寻找依赖的所有 SQL 表, 类定义顺序保证了依赖关系一定是 DAG, 不需要判断循环依赖 """
        return chain(*[cls.collect(tb.dependencies) if tb.is_computed else [tb] for tb in tables])

    @classmethod
    def get_df_no_copy(cls):
        if any(dependant.timestamp > cls.timestamp for dependant in cls.dependencies):
            cls._recompute()
        return cls._df

    @classmethod
    def compute(cls) -> pd.DataFrame:
        """
        compute 方法内必须调用 get_df() 获取依赖表的 dataframe, 调用 Table.df 可能导致多次重复加载依赖表浪费时间.
        compute 方法可能在多个议会线程及业务线程并行执行, 需要注意线程安全问题.
        """
        raise NotImplementedError

    @classmethod
    def before_get_df_hook(cls):
        for dependant in cls.dependencies:
            dependant.before_get_df_hook()

    @classmethod
    async def async_before_get_df_hook(cls):
        get_df_task = lambda tb: asyncio.create_task(tb.async_before_get_df_hook())
        df_tasks = [get_df_task(dependant) for dependant in cls.dependencies]
        await asyncio.gather(*df_tasks)

    @classmethod
    def reload(cls):
        pass        # Computed 表无需 reload, get df 时按需计算

    @classmethod
    def pull_diff_from_db(cls):
        pass        # Computed 表不同步 diff

    @classmethod
    def _recompute(cls):
        try:
            new_df = cls.compute()
            with cls.lock:
                cls._df = new_df
            cls.update_timestamp()
            log_debug(f'更新 computed view [{cls.table_name}] 完成')
        except Exception as e:
            log_error(f'更新 computed view [{cls.table_name}] 失败!', e)

    @classmethod
    def initialize(cls):
        cls.before_get_df_hook()
        cls._recompute()

    @classmethod
    def initialized(cls):
        return cls._df is not None


class InMemoryTable(IDataTable):
    """
    只存在于议会成员内存中的表, 与 DB 脱钩. 注意这种表只处理进程启动后收到的消息, 启动前的消息忽略, 所以启动时间不一致的进程数据可能也是不一致的.
    为了增强数据的一致性, 必须设置主键列.
    可以重写 `init_sql` 方法执行 SQL 从 DB 获取初始 dataframe 的内容, 如获取全部 username 等.
    """
    _df: Optional[pd.DataFrame] = None
    primary_key_columns = None
    data_birth_time: datetime = None
    patch_buffer: list = []

    def __init_subclass__(cls, dependencies=None, primary_key_columns=None, **kwargs):
        assert dependencies is None, f'{cls.__name__} 作为 InMemoryTable 不能有 dependency'
        assert primary_key_columns is not None, f'InMemoryTable {cls.__name__} 必须有主键列'
        super().__init_subclass__(dependencies=dependencies, **kwargs)
        cls.primary_key_columns = primary_key_columns
        cls.patch_buffer = []
        assert len(absent:= [col for col in cls.primary_key_columns if col not in cls.columns]) == 0, \
            f'{cls.__name__} 表的主键列 {absent} 不存在于 columns 中!'

    @classmethod
    def get_df_no_copy(cls):
        return cls._df

    @classmethod
    def init_sql(cls) -> Optional[str]:
        pass

    @classmethod
    def patch(cls, patch: dict):
        if not cls.initialized():
            cls.patch_buffer.append(patch)  # 尚未拿到初始数据, 缓存一下 patch
            return
        to_del, to_add = patch['patch']
        to_del, to_add = to_del.df.set_index(cls.primary_key_columns), to_add.df.set_index(cls.primary_key_columns)
        # to_del 中有些是先删再增实现修改的, 现在有主键了不需要这样做了
        delete_index = to_del.index.difference(to_add.index)
        with cls.lock:
            df = cls._df.set_index(cls.primary_key_columns)
            df = to_add.combine_first(df)                 # 修改或增加行
            df = df.drop(delete_index, errors='ignore')   # 删除行, 数据不一定是一致的, 找不到要删的 index 不报错
            cls._df = df.reset_index()
        cls.update_timestamp(patch.get('timestamp', time.time()))

    @classmethod
    def init_from_scratch(cls):
        try:
            if (sql := cls.init_sql()) is not None:
                cls._df = PatchableDataFrame.load_from_db(sql=sql, get_raw_df=True).drop(['query_timestamp'], axis='columns')
            else:
                cls._df = pd.DataFrame(columns=cls.columns)
            cls.data_birth_time = datetime.now()
        except Exception as e:
            log_error(f'initialized in-mem table {cls.__name__} failed', exception=e)

    @classmethod
    def initialize(cls):
        if os.environ.get('MODULE_NAME') == 'manager':
            cls.init_from_scratch() # manager 不需要全量数据
            return
        key = f'UserData#{str(uuid.uuid4())[:8]}'
        request = {'table_name': cls.table_name, 'key': key}
        cls.receiving_patch = True  # 提前开始接收 patch 缓存起来
        MessageQueue.send(MessageType.IN_MEM_REQUEST, request)
        key = f'{key}_response'
        init_data = redis_conn.brpop(key, timeout=2)
        if init_data is None:
            log_error(f'{cls.table_name} 等待初始数据超时, 由本地生成初始数据')
            cls.init_from_scratch()
        else:
            init_data = pickle.loads(init_data[1])
            cls._df = init_data.get('data')
            cls.data_birth_time = init_data.get('birth_time')
            cls.update_timestamp(init_data.get('timestamp'))
            log_info(f'从 {init_data.get("from")} 获取到了 {cls.table_name} 表的初始化数据, ' +
                f'生成于 {cls.data_birth_time}, age={datetime.now() - cls.data_birth_time}')
        for patch in cls.patch_buffer:
            if patch['timestamp'] > cls.timestamp:
                cls.patch(patch)

    @classmethod
    def initialized(cls):
        return cls._df is not None

    # 以下是 InMemoryTable 不需要的操作
    @classmethod
    def before_get_df_hook(cls):
        pass

    @classmethod
    def reload(cls):
        pass

    @classmethod
    def pull_diff_from_db(cls):
        pass
