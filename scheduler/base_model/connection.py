

import os
import mmap
import pickle
import posix_ipc
from typing import Callable
from collections import namedtuple

from conf import CONF
from scheduler.base_model.base_types import TickData


# 默认 5 个 rotate 位置
DEFAULT_ROTATE_NUM = CONF.scheduler.get('rotate_num', 5)
Header = namedtuple('Header', ['seq', 'frames'])


class ProcessConnection(object):

    def __init__(
            self,
            shm_name,
            rotate_num=DEFAULT_ROTATE_NUM,
            init_obj=TickData(),
            dumps: Callable = TickData.dumps,
            loads: Callable = TickData.loads
    ):
        self.dumps = dumps
        self.loads = loads
        self.rotate_num = rotate_num
        self.header_size = (128 + 16 * rotate_num) * 2 + 1
        self.shm = posix_ipc.SharedMemory(shm_name, posix_ipc.O_CREAT, mode=0o777)
        _init = False
        if self.shm.size == 0:
            _init = True
            os.ftruncate(self.shm.fd, (self.header_size + self.rotate_num * len(self.dumps(init_obj))) * 2)
        self.mm = mmap.mmap(self.shm.fileno(), 0)
        self._size = self.mm.size()
        if _init:
            self.set_header(Header(seq=0, frames=[]))
            for i in range(self.rotate_num):
                self.put(init_obj)

    def set_header(self, header: Header):
        # 我们是只有一个进程一秒钟一个脉冲 put obj，所以这样没有问题
        self.mm.seek(0)
        half_header_length = int(self.header_size / 2)
        if self.mm.read(1) == b'0':
            self.mm.seek(1 + half_header_length)
            self.mm.write(pickle.dumps(header))
            self.mm.seek(0)
            self.mm.write(b'1')
        else:
            self.mm.seek(1)
            self.mm.write(pickle.dumps(header))
            self.mm.seek(0)
            self.mm.write(b'0')

    @property
    def header(self) -> Header:
        self.mm.seek(0)
        _b = self.mm.read(self.header_size)
        if _b[0] == b'0'[0]:
            return pickle.loads(_b[1:int(self.header_size / 2)])
        else:
            return pickle.loads(_b[1+int(self.header_size / 2):])

    def get(self):
        header = self.header
        last_start_position, last_data_length = header.frames[-1]
        if last_start_position + last_data_length >= self._size:
            self.mm.resize(self.mm.size())
        self.mm.seek(last_start_position)
        return self.loads(self.mm.read(last_data_length))

    def put(self, obj, seq=0):
        header = self.header
        pickle_bytes = self.dumps(obj)
        pickle_bytes_length = len(pickle_bytes)
        max_position = self.mm.size()
        frames = sorted(header.frames[-self.rotate_num:]) + [(max_position, 0)]
        position = -1
        last_position = self.header_size
        for p, l in frames:
            if p - last_position >= pickle_bytes_length:
                position = last_position
                break
            last_position = p + l
        if position < 0:
            position = max_position
            self.mm.resize(max_position + pickle_bytes_length)
        self.mm.seek(position)
        self.mm.write(pickle_bytes)
        self.set_header(Header(seq=seq, frames=(header.frames + [(position, pickle_bytes_length)])[-self.rotate_num:]))
