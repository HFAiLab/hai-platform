import time

from db import redis_conn
from .base import BaseMQ


class RedisMulticastMQ(BaseMQ):
    @classmethod
    def send_channel(cls, data, channel, index_key=None, expire=3600):
        index_key = index_key or f'{channel}_index'
        with redis_conn.pipeline() as pipe:
            while True:  # 过程中被打断则继续重试
                try:
                    pipe.watch(index_key)
                    index = pipe.get(index_key)
                    index = 0 if index is None else int(index)
                    pipe.multi()
                    pipe.set(index_key, index + 1)
                    pipe.set(f'{channel}:{index}', data)
                    if expire is not None:
                        pipe.expire(f'{channel}:{index}', expire)
                    pipe.execute()
                    break
                except:
                    time.sleep(0.1)

    @classmethod
    def listen_channel(cls, channel, index_key=None):
        index_key = index_key or f'{channel}_index'
        last_index = redis_conn.get(index_key)
        last_index = 0 if last_index is None else int(last_index)
        while True:
            index = redis_conn.get(index_key)
            index = 0 if index is None else int(index)
            for new_id in range(last_index, index):
                info = redis_conn.get(f'{channel}:{new_id}')
                if info is None:
                    continue
                yield info
            last_index = max(last_index, index)
            time.sleep(1)

    def __init__(self, channel, index_key=None):
        super().__init__(channel)
        self.index_key = index_key

    def send(self, data, expire=3600):
        return self.__class__.send_channel(data=data, channel=self.channel, index_key=self.index_key, expire=expire)

    def listen(self):
        yield from self.__class__.listen_channel(channel=self.channel, index_key=self.index_key)
