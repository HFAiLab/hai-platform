



import pandas as pd

from conf.flags import TASK_TYPE
from scheduler.base_model import Matcher
from .match_jupyter_task import match_task as match_jupyter_task


class JupyterMatcher(Matcher):
    re_signal_where = f''' "unfinished_task_ng"."task_type" = '{TASK_TYPE.JUPYTER_TASK}' '''

    def __init__(self, **kwargs):
        self.match_jupyter_task = match_jupyter_task
        super(JupyterMatcher, self).__init__(**kwargs)

    def process_match(self):
        # 这里等一下 training 的下次数据，重复数据 match 无意义
        jupyter_result = self.waiting_for_upstream_data()
        self.set_tick_data(jupyter_result)
        self.resource_df = self.resource_df[self.resource_df.active]
        task_df = pd.DataFrame(columns=self.task_df.columns)
        r, t = self.match_jupyter_task(self.resource_df.copy(), jupyter_result.task_df, jupyter_result.extra_data)
        self.resource_df = pd.concat([self.resource_df, r])
        self.resource_df.drop_duplicates(subset=['name'], keep='last', inplace=True)
        task_df = pd.concat([task_df, t])
        # 全部 match 结束把 task_df 去重，理论上没有重复，出现重复以第一个 matcher 为准
        task_df.drop_duplicates(subset=['id'], keep='first', inplace=True)
        self.task_df = task_df
