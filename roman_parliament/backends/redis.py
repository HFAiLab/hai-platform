from conf import CONF
from .base import BaseBackend
from .message_queue import RedisMulticastMQ, RedisUnicastMQ
from roman_parliament.utils import is_senator, generate_key
from roman_parliament.mass import get_mass_set, get_mass_info
import pickle
import time


def wrap_with_pickle_load(generator):
    for value in generator:
        yield pickle.loads(value)


class RedisBackend(BaseBackend):
    senator_mq = RedisMulticastMQ(channel=CONF.parliament.info_channel, index_key=CONF.parliament.monitor_count)

    @classmethod
    def watch(cls):
        if is_senator():
            yield from wrap_with_pickle_load(cls.senator_mq.listen())
        else:
            while (mass_name := get_mass_info()[1]) is None:
                time.sleep(5)
            yield from wrap_with_pickle_load(RedisUnicastMQ.listen_channel(channel=f'mass:{mass_name}'))

    @classmethod
    def set(cls, info, mass=False):
        b_info = pickle.dumps(info)
        cls.senator_mq.send(b_info, expire=60*60)
        if mass:  # 如果本次更新还需要告知群众
            data = info['data']
            # 找到档案对应的key
            key = generate_key(data['class'], data['validate_attr'], data['validate_value'])
            # 找到有哪些群众订阅了这个key
            mass_set = get_mass_set()[key]
            # 对订阅了这个key的群众发消息，告知可以了
            for mass_name in mass_set:
                RedisUnicastMQ.send_channel(b_info, channel=f'mass:{mass_name}', expire=60*60) # 给群众1小时的获取时间
