

import atexit
import pickle
import sysv_ipc
from typing import Callable
from collections import namedtuple

from conf import CONF
from scheduler.base_model.base_types import TickData


# 默认 5 个 rotate 位置，1 G 空间
DEFAULT_ROTATE_NUM = CONF.scheduler.get('rotate_num', 5)
DEFAULT_SHM_SIZE = CONF.scheduler.get('shm_size', 1 << 30)
Header = namedtuple('Header', ['position', 'seq', 'data_length'])


class ProcessConnection(object):

    def __init__(
            self,
            shm_id=None,
            rotate_num=DEFAULT_ROTATE_NUM,
            shm_size=DEFAULT_SHM_SIZE,
            init_obj=TickData(),
            dumps: Callable = TickData.dumps,
            loads: Callable = TickData.loads
    ):
        if shm_id is None:
            self.shm = sysv_ipc.SharedMemory(shm_id, sysv_ipc.IPC_CREX, mode=0o777, size=shm_size)
            atexit.register(self.shm.remove)
        else:
            self.shm = sysv_ipc.SharedMemory(shm_id)
        self.dumps = dumps
        self.loads = loads
        self.rotate_num = rotate_num
        self.header_size = 128
        # 0 ~ header_size 为 header，之后的 size 按照 rotate_num 均分切块
        self.position_offset = [int((shm_size - self.header_size) / rotate_num) * i + self.header_size for i in range(self.rotate_num)]
        if shm_id is None:
            self.set_header(Header(position=0, seq=0, data_length=0))
            for i in range(self.rotate_num):
                self.put(init_obj)

    def set_header(self, header: Header):
        self.shm.write(pickle.dumps(header))

    @property
    def shm_id(self):
        return self.shm.key

    @property
    def header(self) -> Header:
        return pickle.loads(self.shm.read(self.header_size))

    def get(self):
        header = self.header
        return self.loads(self.shm.read(header.data_length, offset=self.position_offset[header.position]))

    def put(self, obj, seq=0):
        header = self.header
        pickle_bytes = self.dumps(obj)
        next_position = (header.position + 1) % self.rotate_num
        self.shm.write(pickle_bytes, offset=self.position_offset[next_position])
        self.set_header(Header(position=next_position, seq=seq, data_length=len(pickle_bytes)))
