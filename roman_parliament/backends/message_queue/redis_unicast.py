from db import redis_conn
from .base import BaseMQ


class RedisUnicastMQ(BaseMQ):
    @classmethod
    def send_channel(cls, data, channel, expire=3600):
        redis_conn.lpush(channel, data)
        if expire is not None:
            redis_conn.expire(channel, expire)

    @classmethod
    def listen_channel(cls, channel):
        while True:
            data = redis_conn.brpop(channel)
            yield data[1]

    def send(self, data, expire=3600):
        return self.__class__.send_channel(data=data, channel=self.channel, expire=expire)
