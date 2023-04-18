

import asyncio
import inspect
import os
import time
import datetime
from typing import Dict, List

import sqlalchemy
import sqlparams
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine

from conf import CONF
from logm import logger


PG_APPLICATION_NAME = f"multi-server-{os.environ.get('MODULE_NAME', 'default')}"


def get_db_engine(db_name):
    db_url = 'postgresql://{user}:{password}@{host}:{port}/{db}?application_name={PG_APPLICATION_NAME}'.format(**CONF.database.postgres[db_name], PG_APPLICATION_NAME=PG_APPLICATION_NAME)
    return create_engine(db_url, pool_pre_ping=True, pool_size=CONF.database.postgres[db_name].pool_size)


def get_async_db_engine(db_name):
    db_url = 'postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}'.format(**CONF.database.postgres[db_name])
    # command_timeout 只有 async 的好使，同步的尽量不要 apply_remote
    return create_async_engine(db_url, pool_pre_ping=True, pool_size=CONF.database.postgres[db_name].pool_size, connect_args={"server_settings": {"application_name": PG_APPLICATION_NAME}})


sql_params = sqlparams.SQLParams(in_style='format', out_style='named')


class MarsDB(object):
    """
    提供了等待从库接收事务的接口
    需要配合数据库 synchronous_standby_names 参数
    patch 了 conn.execute，不需要处理，直接传入 execute(sql: str, params: tuple) 就行（也只接受 str, tuple 的参数）
    """
    __db: Dict[str, sqlalchemy.engine.base.Engine] = {
        'primary': None,
        'secondary': None
    }
    __a_db: Dict[str, sqlalchemy.ext.asyncio.engine.AsyncEngine] = {
        'primary': None,
        'secondary': None
    }
    use_db = 'primary'
    if os.environ.get('MODULE_NAME', '') in ['query-server', 'monitor-server']:
        use_db = 'secondary'
    fallback_primary_time = None
    # 从库出错后，fallback 到 主库 / local 的时间
    FALLBACK_SECONDS = 60 * 60
    # 记录目前的 context
    __contexts = {}

    def __init__(self, overwrite_use_db: str = None):
        # 使用从库
        assert overwrite_use_db in {None, 'secondary', 'primary'}, 'overwrite_use_db 只能是 None / secondary / primary'
        self.overwrite_use_db = overwrite_use_db
        self.__transaction = None
        self.__frame_id = None

    @classmethod
    def dispose(cls):
        try:
            cls.__db['primary'].dispose()
            cls.__db['secondary'].dispose()
        except Exception as e:
            logger.error(e)
        # a_db 暂时不管了

    @classmethod
    def init_db_engine(cls, dispose=True):
        if dispose:
            cls.dispose()
        cls.__db['primary'] = get_db_engine('primary')
        cls.__a_db['primary'] = get_async_db_engine('primary')
        try:
            cls.__db['secondary'] = get_db_engine('secondary')
            cls.__a_db['secondary'] = get_async_db_engine('secondary')
        except Exception:
            cls.__db['secondary'] = cls.__db['primary']
            cls.__a_db['secondary'] = cls.__a_db['primary']

    @classmethod
    def check_fallback_status(cls):
        if cls.fallback_primary_time is not None and time.time() - cls.fallback_primary_time > cls.FALLBACK_SECONDS:
            logger.info('切回可以使用 / 等待从库')
            cls.init_db_engine()
            cls.fallback_primary_time = None

    @property
    def db(self) -> sqlalchemy.engine.base.Engine:
        self.__class__.check_fallback_status()
        # 还在 fallback_to_primary 过程中
        if self.__class__.fallback_primary_time is not None:
            return self.__class__.__db['primary']
        if self.overwrite_use_db is not None:
            return self.__class__.__db[self.overwrite_use_db]
        return self.__class__.__db[self.__class__.use_db]

    @property
    def a_db(self) -> sqlalchemy.ext.asyncio.engine.AsyncEngine:
        self.__class__.check_fallback_status()
        # 还在 fallback_to_primary 过程中
        if self.__class__.fallback_primary_time is not None:
            return self.__class__.__a_db['primary']
        if self.overwrite_use_db is not None:
            return self.__class__.__a_db[self.overwrite_use_db]
        return self.__class__.__a_db[self.__class__.use_db]

    def catch_fallback_to_primary(self, e: Exception):
        # 看要不要退回到主库
        if self.overwrite_use_db == 'secondary' or self.__class__.use_db == 'secondary':
            fallback_exceptions = [ConnectionRefusedError, sqlalchemy.exc.OperationalError]
            fallback_msgs = [
                "Is the server running", "could not translate host name", "Connect call failed", "Connection refused", "connection to server at"
            ]
            if any(isinstance(e, exc) for exc in fallback_exceptions):
                if any(m in str(e) for m in fallback_msgs):
                    msg = f'{os.environ.get("MODULE_NAME", "")}: 出现连接错误，尝试 fallback 到 不使用 & 不等待 从库。exception: {e}'
                    logger.f_error(msg)
                    self.__class__.fallback_primary_time = time.time()
                    return
        raise e

    def execute(self, sql: str, params: tuple = ()) -> sqlalchemy.engine.cursor.CursorResult:
        return self.execute_many(sql_list=[sql], params_list=[params])[0]

    def __execute_many(self, sql_list: List[str], params_list: List[tuple]) -> List[sqlalchemy.engine.cursor.CursorResult]:
        results = []
        with self as conn:
            for sql, params in zip(sql_list, params_list):
                results.append(conn.execute(sql, params))
            return results

    def execute_many(self, sql_list: List[str], params_list: List[tuple]) -> List[sqlalchemy.engine.cursor.CursorResult]:
        try:
            return self.__execute_many(sql_list, params_list)
        except Exception as e:
            if 'canceling statement due to conflict with recovery' in str(e):
                return self.__execute_many(sql_list, params_list)
            raise e

    async def a_execute(self, sql: str, params: tuple = (), remote_apply: bool = False, timeout: int = 10) -> sqlalchemy.engine.cursor.CursorResult:
        return (await self.a_execute_many(sql_list=[sql], params_list=[params], remote_apply=remote_apply, timeout=timeout))[0]

    async def __a_execute_many(self, sql_list: List[str], params_list: List[tuple], remote_apply: bool, timeout: int) -> List[sqlalchemy.engine.cursor.CursorResult]:
        assert len(sql_list) == len(params_list), 'sql 和 params 不等长'
        remote_apply = self.__class__.fallback_primary_time is None and remote_apply
        results = []
        async with self as conn:
            if remote_apply:
                await conn.execute("set local synchronous_commit to 'remote_apply'")
                # 这个理论上没啥用，只是怕一直拿不到锁把程序卡死
                await conn.execute(f"set lock_timeout = 15000")
            for sql, params in zip(sql_list, params_list):
                results.append(await conn.execute(sql, params))
            if remote_apply:
                # remote apply 手动 commit，timeout 就掐掉
                fut = asyncio.ensure_future(conn.commit())
                try:
                    await asyncio.wait_for(fut, timeout)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    msg = f'{os.environ.get("MODULE_NAME", "")}: 等待从库超时，尝试 fallback 到 不使用 & 不等待 从库。'
                    logger.f_error(msg)
                    self.__class__.fallback_primary_time = time.time()
                    if not fut.done():
                        raise
            return results

    async def a_execute_many(self, sql_list: List[str], params_list: List[tuple], remote_apply: bool = False, timeout: int = 10) -> List[sqlalchemy.engine.cursor.CursorResult]:
        try:
            return await self.__a_execute_many(sql_list, params_list, remote_apply, timeout)
        except Exception as e:
            if 'canceling statement due to conflict with recovery' in str(e):
                return await self.__a_execute_many(sql_list, params_list, remote_apply, timeout)
            raise e

    def __enter__(self) -> sqlalchemy.engine.base.Connection:
        try:
            self.__transaction = self.db.begin()
            conn = self.__transaction.__enter__()
        except Exception as e:
            self.catch_fallback_to_primary(e)
            self.__transaction = self.db.begin()
            conn = self.__transaction.__enter__()
        return conn

    def __exit__(self, *exc):
        return self.__transaction.__exit__(*exc)

    async def __aenter__(self) -> sqlalchemy.ext.asyncio.engine.AsyncConnection:
        try:
            self.__transaction = self.a_db.begin()
            conn = await self.__transaction.__aenter__()
        except Exception as e:
            self.catch_fallback_to_primary(e)
            self.__transaction = self.a_db.begin()
            conn = await self.__transaction.__aenter__()
        return conn

    async def __aexit__(self, *exc):
        return await self.__transaction.__aexit__(*exc)


# patch connection, 只支持 sql, params 参数调用
def __execute(self, statement, *multiparams, **params):
    if isinstance(statement, sqlalchemy.sql.elements.TextClause):
        return self._execute(statement, *multiparams, **params)
    sql, params = sql_params.format(statement, multiparams[0]) if len(multiparams) > 0 else (statement, ())
    return self._execute(sqlalchemy.text(sql), params)


async def __a_execute(self, statement, parameters=None, execution_options=None, **kwargs):
    if isinstance(statement, sqlalchemy.sql.elements.TextClause):
        return await self._execute(statement, parameters)
    sql, params = sql_params.format(statement, parameters) if parameters else (statement, ())
    return await self._execute(sqlalchemy.text(sql), params)


sqlalchemy.engine.base.Connection._execute = sqlalchemy.engine.base.Connection.execute
sqlalchemy.ext.asyncio.engine.AsyncConnection._execute = sqlalchemy.ext.asyncio.engine.AsyncConnection.execute
sqlalchemy.engine.base.Connection.execute = __execute
sqlalchemy.ext.asyncio.engine.AsyncConnection.execute = __a_execute

MarsDB.init_db_engine(dispose=False)
