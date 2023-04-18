

from .base_processor import BaseProcessor


class Subscriber(BaseProcessor):
    """
    Subscriber 模块，订阅若干需要的模块，进行操作
    """

    def __init__(self, **kwargs):
        super(Subscriber, self).__init__(**kwargs)

    def user_tick_process(self):
        self.process_subscribe()

    def process_subscribe(self):
        raise NotImplementedError
