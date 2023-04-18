
import time
import pandas as pd
import sqlalchemy

from db import MarsDB


class PatchConflictException(Exception):
    def __init__(self, *args):
        super().__init__(*args)
        self.table_name = None


class PatchableDataFrame(object):
    def __init__(self, df=None, df_indexing=None, timestamp=0):
        """
            timestamp: 上次从 DB 中拉取完整数据的 timestamp, 应用 patch 不会更新.
                        用于防止从数据库拉取最新数据后再收到的过时 patch 被应用.
            df_indexing: - list, dict 等 unhashable 类型不能用于比较 diff, 因此转为 string 来比较;
                         - 而逆向转换很难做, 也费时间, 所以同时保留原始的 df 和转换后的 df_indexing
        """
        self.df = df
        self.timestamp = timestamp
        if df_indexing is None:
            self.df_indexing = df.copy()
            obj_columns = df.columns[df.dtypes == object]
            self.df_indexing[obj_columns] = self.df_indexing[obj_columns].astype(str, copy=False)
            datetime_columns = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col].dtype)]
            for col in datetime_columns:
                transform = lambda t: f'{t.timestamp():.6f}' if not pd.isna(t) else ''
                self.df_indexing[col] = self.df_indexing[col].apply(transform)
        else:
            self.df_indexing = df_indexing

    @classmethod
    def from_df(cls, df):
        timestamp = df['query_timestamp'][0].timestamp() if len(df) > 0 else time.time()
        df = df.drop(['query_timestamp'], axis='columns')
        return cls(df=df, timestamp=timestamp)

    @classmethod
    def load_from_db(cls, sql: str, get_raw_df=False):
        df = pd.read_sql(sqlalchemy.text(sql), MarsDB(overwrite_use_db='primary').db)
        return df if get_raw_df else cls.from_df(df)

    @classmethod
    async def async_load_from_db(cls, sql: str, columns, get_raw_df=False):
        result = await MarsDB(overwrite_use_db='primary').a_execute(sql)
        df = pd.DataFrame(result, columns=columns + ['query_timestamp'])
        return df if get_raw_df else cls.from_df(df)

    def index_select(self, indices):
        return PatchableDataFrame(df=self.df.loc[indices].copy(),
                                  df_indexing=self.df_indexing.loc[indices].copy(),
                                  timestamp=self.timestamp)

    def diff(self, df_new):
        df_new_indexing = df_new.df_indexing.reset_index().rename(columns={'index': 'rindex'})
        merged_df = self.df_indexing.reset_index().merge(df_new_indexing, how='outer', indicator=True)
        to_del_indices = merged_df[merged_df._merge == 'left_only']['index'].astype(int)
        to_add_indices = merged_df[merged_df._merge == 'right_only']['rindex'].astype(int)
        if len(to_del_indices) + len(to_add_indices) > 0:
            return self.index_select(to_del_indices), df_new.index_select(to_add_indices)   # to_del, to_add
        else:
            return None

    def apply_patch(self, to_del, to_add):
        # check patch sanity
        merge_w_to_del = self.df_indexing.reset_index().merge(to_del.df_indexing, how='outer', indicator=True)
        merge_w_to_add = self.df_indexing.reset_index().merge(to_add.df_indexing, how='outer', indicator=True)
        if len(merge_w_to_del[merge_w_to_del._merge == 'right_only']) > 0:
            raise PatchConflictException("找不到要删除的 data row")
        if len(merge_w_to_add[merge_w_to_add._merge == 'both']) > 0:
            raise PatchConflictException("要添加的 data row 已经存在于 df 中")
        try:
            # remove rows to delete && add new rows
            keep_indices = merge_w_to_del[merge_w_to_del._merge == 'left_only']['index'].dropna().astype(int)
            df = self.df.loc[keep_indices].copy()
            self.df = pd.concat([df, to_add.df]).reset_index(drop=True)
            df_indexing = self.df_indexing.loc[keep_indices].copy()
            self.df_indexing = pd.concat([df_indexing, to_add.df_indexing]).reset_index(drop=True)
        except Exception as e:
            # 可能是表字段改变等 corner case, raise 后直接从数据库重新拉数据
            raise PatchConflictException(f'添加/删除 rows 时出错: {e}') from e
