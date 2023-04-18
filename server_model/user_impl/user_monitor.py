
from __future__ import annotations

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from itertools import product
from typing import TYPE_CHECKING, Optional
from munch import Munch

import aiofiles.os
import cachetools
import os
import pandas as pd
import ujson

from conf import CONF
from base_model.base_user_modules import IUserMonitor
from conf.flags import QUE_STATUS, TASK_TYPE
from db import MarsDB
from monitor import async_get_storage_usage_at, StorageTypes
from logm import ExceptionWithoutErrorLog

if TYPE_CHECKING:
    from server_model.user import User


DF_CACHE = cachetools.LRUCache(maxsize=128)
TASK_MODES = ['realtime', 'daily', 'weekly', 'monthly']
PRIORITY_MODES = ['7d', '30d', '60d']
ModeToTimeDeltas = {
    'realtime': timedelta(hours=1),
    'daily': timedelta(days=1),
    'weekly': timedelta(days=7),
    'monthly': relativedelta(months=1),
}
TaskStatDirFormat = {
    'realtime': '%Y%m%d%H',
    'daily': '%Y%m%d',
    'weekly': '%Y%m%d',
    'monthly': '%Y%m',
}


async def async_read_df(file_path) -> Optional[pd.DataFrame]:
    file_path = os.path.join(CONF.user_data_roaming.cluster_report_dir, file_path)
    if (df := DF_CACHE.get(file_path)) is not None:
        return df
    if not (await aiofiles.os.path.exists(file_path)):
        return None
    async with aiofiles.open(file_path) as f:
        df = pd.DataFrame(ujson.loads(await f.read()))
        df = df.astype(object).where(pd.notnull(df), None)      # 去除 NaN
    DF_CACHE[file_path] = df
    return df


async def get_task_stat_df_for_tick(tick: datetime, mode: str, cluster_type: str, retried = False) -> Optional[Munch]:
    dirname = (tick - ModeToTimeDeltas[mode]).strftime(TaskStatDirFormat[mode])
    df = await async_read_df(os.path.join(mode, dirname, f'report_{cluster_type}.json'))
    if df is None and not retried:
        # 获取最新报告时, 若无结果, 可能当前时间段的报告正在生成, 先用老的
        tick = tick - ModeToTimeDeltas[mode]
        return await get_task_stat_df_for_tick(tick, mode, cluster_type, retried=True)
    return Munch(df=df, dirname=dirname)


async def get_priority_stat_df_for_tick(tick: datetime, mode: str, cluster_type: str, retried = False) -> Optional[Munch]:
    tick = tick - timedelta(days=1)    # 获取到昨天为止, 7/30/60 天的数据
    dirname = (tick - timedelta(days=int(mode[:-1]))).strftime('%Y%m%d') + '_' + tick.strftime('%Y%m%d')
    df = await async_read_df(os.path.join('priority', mode, dirname, f'report_{cluster_type}.json'))
    if df is None and not retried:
        # 获取最新报告时, 若无结果, 可能当前时间段的报告正在生成, 先用老的
        tick = tick - timedelta(days=1)
        return await get_priority_stat_df_for_tick(tick, mode, cluster_type, retried=True)
    return Munch(df=df, dirname=dirname)


async def get_storage_stat_df_for_tick(tick: datetime, storage_type):
    df = pd.DataFrame(await async_get_storage_usage_at(tick=tick, storage_type=storage_type))
    df = df.astype(object).where(pd.notnull(df), None)
    return df if len(df) > 0 else None


COUNT_CACHE = cachetools.TTLCache(ttl=120, maxsize=300)
async def async_count_user_tasks(user_name):
    if COUNT_CACHE.get(user_name) is None:
        sql = '''select count(*) from "unfinished_task_ng" where "queue_status" = %s and "user_name" = %s and task_type = %s '''
        n_running = await MarsDB().a_execute(sql=sql, params=(QUE_STATUS.SCHEDULED, user_name, TASK_TYPE.TRAINING_TASK))
        n_queued =  await MarsDB().a_execute(sql=sql, params=(QUE_STATUS.QUEUED, user_name, TASK_TYPE.TRAINING_TASK))
        COUNT_CACHE[user_name] = {'running': n_running.fetchone()[0], 'queued': n_queued.fetchone()[0]}
    return COUNT_CACHE[user_name]


class UserMonitor(IUserMonitor):
    def __init__(self, user: User):
        super().__init__(user)
        self.user = user

    async def async_get(self):
        tick = datetime.now()
        stat = {
            'task': {(mode, cluster): await self.async_task_stat(tick=tick, mode=mode, cluster_type=cluster)
                     for mode, cluster in product(TASK_MODES, ['cpu', 'gpu']) },
            'priority': {(mode, cluster): await self.async_priority_stat(tick=tick, mode=mode, cluster_type=cluster)
                         for mode, cluster in product(PRIORITY_MODES, ['cpu', 'gpu'])},
        }
        result = {
            split : {
                mode : { cluster: stat[split][(mode, cluster)] for cluster in ['cpu', 'gpu'] } for mode in modes
            } for split, modes in zip(['task', 'priority'], [TASK_MODES, PRIORITY_MODES])
        }
        result['storage'] = {
            storage_type: await self.async_storage_stat(tick=tick, storage_type=storage_type)
            for storage_type in StorageTypes
        }
        result['task_count'] = await self.async_task_count()
        return result

    async def async_task_stat(self, tick: datetime, mode: str = 'daily', cluster_type: str = 'gpu') -> Optional[Munch]:
        """ 获取 tick 所在小时/天/周/月 的统计数据 """
        if mode not in TASK_MODES:                  raise ExceptionWithoutErrorLog(f'invalid mode {mode}')
        if cluster_type not in ['cpu', 'gpu']:      raise ExceptionWithoutErrorLog(f'invalid cluster_type {cluster_type}')
        if (res := await get_task_stat_df_for_tick(tick, mode, cluster_type)).df is not None and \
                len(df := res.df[res.df.username == self.user.user_name]) > 0:
            return Munch(data=df.iloc[0].to_dict(), data_split=res.dirname)
        else:
            return None

    async def async_priority_stat(self, tick: datetime, mode: str = '7d', cluster_type: str='gpu') -> Optional[Munch]:
        """ 获取以 tick 当日及之前 7/30/60 天的优先级统计数据 """
        if mode not in PRIORITY_MODES:              raise ExceptionWithoutErrorLog(f'invalid mode {mode}')
        if cluster_type not in ['cpu', 'gpu']:      raise ExceptionWithoutErrorLog(f'invalid cluster_type {cluster_type}')
        if (res := await get_priority_stat_df_for_tick(tick, mode, cluster_type)).df is not None and \
                len(df := res.df[res.df.username == self.user.user_name]) > 0:
            return Munch(data=df.iloc[0].to_dict(), data_split=res.dirname)
        else:
            return None

    async def async_storage_stat(self, tick: datetime, storage_type: str) -> Munch:
        """ 获取 tick 前最近一次打点数据 """
        if storage_type not in StorageTypes: raise ExceptionWithoutErrorLog(f'invalid storage_type {storage_type}')
        if (res := await get_storage_stat_df_for_tick(tick, storage_type)) is not None:
            df = res.transpose()
            user_items = df[(df.index + '/').str.contains(f'/{self.user.user_name}/')].to_dict('index')
            user_shared_group_items = df[(df.index + '/').str.contains(f'/{self.user.shared_group}/')].to_dict('index')
            return Munch(user_items=user_items, user_shared_group_items=user_shared_group_items)
        else:
            return Munch(user_items={}, user_shared_group_items={})

    async def async_task_count(self):
        return await async_count_user_tasks(self.user.user_name)
