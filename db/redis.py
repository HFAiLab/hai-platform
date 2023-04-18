

import time

import aioredis
import redis
import ujson

from conf import CONF

redis_conn = redis.Redis(**CONF.database.redis)  # 同步的redis


redis_config = CONF.database.redis
a_redis = aioredis.from_url(
        f'redis://{redis_config["host"]}:{redis_config["port"]}',
        db=redis_config["db"], password=redis_config.get('password', None),
    )


def add_timestamp(func):
    def handle_problems(name, *values):
        new_values_list = []
        for value in values:
            try:
                new_value = ujson.loads(value)
                if isinstance(new_value, dict):
                    new_value['hf_timestamp'] = time.time()
                new_value = ujson.dumps(new_value)
            except:
                new_value = value
            new_values_list.append(new_value)
        return func(name, *new_values_list)
    return handle_problems


setattr(redis_conn, 'lpush', add_timestamp(getattr(redis_conn, 'lpush')))
setattr(a_redis, 'lpush', add_timestamp(getattr(a_redis, 'lpush')))
