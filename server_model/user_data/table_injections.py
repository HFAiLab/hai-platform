
"""
把一些需要调用 UserData 类的方法注入到 DataTable 类里, 方便使用.
    (DataTable 类里直接实现这些方法会有循环依赖问题)
"""
import contextlib
import time
import traceback
from typing import Type

import pandas as pd

from .data_table import InMemoryTable
from .public_data_table import PublicDataTable
from .patchable_dataframe import PatchableDataFrame
from .tools import get_user_data_instance
from .utils import log_warning


class DfDescriptor:
    def __get__(self, obj, obj_type):
        return get_user_data_instance().get_df(obj_type.table_name)


class AsyncDfDescriptor:
    def __get__(self, obj, obj_type):
        return get_user_data_instance().async_get_df(obj_type.table_name)


class DiffRecorder:
    def __init__(self, table_cls: Type[PublicDataTable]):
        self.cls = table_cls.private_table
        # 必须在 yield 前 snapshot 一下 df, 否则如果议会线程中途修改了 df 最后会导致额外的 diff 形成
        self.df_snapshot = table_cls.df     # 必须用 .df 来拿 df, 这样表没有初始化的时候会进行订阅和初始化
        self.committed = False

    def commit(self, modified_df: pd.DataFrame):
        assert not self.committed, '不允许一个 context scope 内多次 commit'
        assert modified_df.set_index(self.cls.primary_key_columns).index.is_unique, f'修改后的 {self.cls.__name__} 表主键不唯一'
        diff = PatchableDataFrame(df=self.df_snapshot).diff(PatchableDataFrame(df=modified_df))
        if diff is not None:
            patch = {'table_name': self.cls.table_name, 'patch': diff, 'timestamp': time.time()}
            get_user_data_instance().patch([patch], broadcast=True)
        self.committed = True


# noinspection PyDecorator
@classmethod
@contextlib.contextmanager
def modify(cls: Type[PublicDataTable]):
    recorder = DiffRecorder(table_cls=cls)
    assert issubclass(cls.private_table, InMemoryTable), '只能通过此方法修改 InMemoryTable'
    try:
        yield recorder.df_snapshot.copy(), recorder.commit
    except Exception as e:
        raise e
    if not recorder.committed:
        tb = ''.join(traceback.format_stack(limit=10)[:-1])
        log_warning('调用了 modify 但没有 commit change, stack trace 见下\n' + tb)


PublicDataTable.df = DfDescriptor()
PublicDataTable.async_df = AsyncDfDescriptor()
PublicDataTable.modify = modify
