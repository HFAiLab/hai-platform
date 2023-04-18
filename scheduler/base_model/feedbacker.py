

import datetime
import time

from .base_types import TickData
from .base_processor import BaseProcessor


class FeedBacker(BaseProcessor):
    """
    FeedBacker 模块，用于每隔 interval 的毫秒数，提交调度获取 df 的修改意见
    """
    def __init__(self, *, interval, **kwargs):
        self.interval = interval
        super(FeedBacker, self).__init__(**kwargs)

    def user_tick_process(self):
        self.extra_data = {}
        self.process_modifier()
        self.set_tick_data(TickData(
            seq=int(datetime.datetime.now().timestamp() * 1000),
            valid=self.valid,
            extra_data=self.extra_data
        ))
        time.sleep(self.interval / 1000)

    def process_modifier(self):
        raise NotImplementedError
