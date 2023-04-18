

from .base_processor import BaseProcessor


class Assigner(BaseProcessor):
    """
    Assigner 模块，规定唯一上游为 Beater，负责处理 TickData，产出可以运行的任务
    """

    def __init__(self, **kwargs):
        super(Assigner, self).__init__(**kwargs)

    def user_tick_process(self):
        # 等待 beater 下一次 tick_data
        self.set_tick_data(self.waiting_for_upstream_data())
        # 开始调度
        self.process_schedule()

    def process_schedule(self):
        raise NotImplementedError
