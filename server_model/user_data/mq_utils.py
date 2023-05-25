import pickle
import time
import uuid
from threading import Thread
from typing import TYPE_CHECKING

from conf import CONF
from roman_parliament.backends.message_queue import RedisMulticastMQ
from .utils import log_debug, log_info, log_error

if TYPE_CHECKING:
    from .user_data import UserData


class MessageType:
    PATCH = 'patch'
    SYNC = 'sync'
    RELOAD = 'reload'
    IN_MEM_REQUEST = 'in_mem_req'


class MessageQueue:
    origin_id = str(uuid.uuid4())
    channel = CONF.user_data_roaming.message_queue_channel

    @classmethod
    def listen(cls):
        for data in RedisMulticastMQ.listen_channel(channel=cls.channel):
            yield pickle.loads(data)

    @classmethod
    def send(cls, type, data):
        msg = {'type': type, 'data': data, 'origin': cls.origin_id}
        RedisMulticastMQ.send_channel(data=pickle.dumps(msg), channel=cls.channel, expire=3600)


class WatchThread(Thread):
    def __init__(self, user_data: "UserData"):
        super().__init__(daemon=True)
        self.user_data = user_data

    def process(self, msg):
        msg_type, data, origin = msg.get('type'), msg.get('data'), msg.get('origin')
        if msg_type == MessageType.PATCH and origin != MessageQueue.origin_id:
            log_debug(f'通过议会收到 patch about table {[p["table_name"] for p in data]}')
            self.user_data.patch(data, broadcast=False)
        if msg_type == MessageType.SYNC:
            self.user_data.sync_from_db(data)
        if msg_type == MessageType.RELOAD and origin != MessageQueue.origin_id:
            log_info(f'通过议会收到 reload 信号, 重新加载 df ({data})')
            self.user_data.reload_dfs()
        if msg_type == MessageType.IN_MEM_REQUEST and origin != MessageQueue.origin_id:
            self.user_data.respond_in_mem_table_request(**data)

    def run(self):
        while True:
            msg = None
            try:
                for msg in MessageQueue.listen():
                    self.process(msg)
            except Exception as e:
                log_error(f'获取或处理 msg 失败: {msg}', e)
                time.sleep(1)
