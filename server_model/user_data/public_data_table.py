
from enum import Enum
from typing import Dict, Type, Awaitable, Optional, Callable, ContextManager, Tuple, List

import pandas as pd

from .data_table import IDataTable
from .data_table import RoamingBaseTable, DBBaseTable, RoamingSqlTable, DBSqlTable


"""
Public Table:   暴露给 UserData 模块外部的 Table 类, 外部代码按表访问用户数据的入口
Private Table:  与 public 相对, 在 UserData 模块内部使用的功能性类
"""


class AutoTable(Enum):
    AutoSqlTable = 'auto_sql_table'
    AutoBaseTable = 'auto_base_table'


def translate_base_class(table_class, enable_parliament):
    if table_class == AutoTable.AutoBaseTable:
        return RoamingBaseTable if enable_parliament else DBBaseTable
    elif table_class == AutoTable.AutoSqlTable:
        return RoamingSqlTable if enable_parliament else DBSqlTable
    return table_class


def spawn_private_table(public_cls, enable_parliament):
    assert public_cls.private_table is None, '不允许多次 spawn private tables, 可能进行了多次初始化'
    if 'dependencies' in (init_kwargs := public_cls.init_kwargs):
        # 把 dependency 中引用的 public table 类转换为对应的 private table 类
        init_kwargs['dependencies'] = [dep.private_table for dep in init_kwargs.get('dependencies')]
    private_cls = type(public_cls.__name__, (translate_base_class(public_cls.table_cls, enable_parliament),),
                       dict(public_cls.__dict__), **init_kwargs)
    # noinspection PyTypeChecker
    public_cls.private_table = private_cls
    # noinspection PyUnresolvedReferences
    public_cls.get_df = public_cls.private_table.get_df


class PublicDataTable:
    # 外部访问的接口, 在 ./table_injections.py 中定义并设置, 否则会有循环依赖问题, 这里仅设置类型方便代码补全
    df: Optional[pd.DataFrame] = None
    async_df: Optional[Awaitable[pd.DataFrame]] = None
    modify: Optional[Callable[[], ContextManager[Tuple[pd.DataFrame, Callable[[pd.DataFrame], None]]]]] = None

    # private table 的一些属性或方法, 外部代码可能会引用
    table_name: str = None
    columns: List[str] = None
    private_key_columns: List[str] = None

    # private table 相关
    table_cls: Optional[Type[IDataTable]] = None
    init_kwargs: Dict = None
    private_table: Optional[Type[IDataTable]] = None
    get_df: Callable[[], pd.DataFrame] = None

    def __init_subclass__(cls, table_cls=None, **kwargs):
        cls.table_cls = table_cls
        cls.init_kwargs = kwargs
        cls.table_name = kwargs.get('table_name')
        cls.columns = kwargs.get('columns')
        cls.private_key_columns = kwargs.get('private_key_columns')
