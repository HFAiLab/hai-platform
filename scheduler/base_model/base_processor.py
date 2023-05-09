import functools
import inspect
import os
import time
from collections import defaultdict
from typing import Dict

import pandas as pd
import ujson

from logm import logger
from k8s import K8sPreStopHook
from .base_types import TickData
from .connection import ProcessConnection


class TickDataDescriptor(object):

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        return getattr(instance.tick_data, self.name)

    def __set__(self, instance, value):
        return setattr(instance.tick_data, self.name, value)


class BaseProcessor(object):
    """
    提供调度组件的基类方法，包括 订阅消息、发布消息 等
    name: 该组件的名字，用于区分 log
    upstreams: 该组件的上游，可以有多个，dict 的 key 是名字，value 是对应的 Reader
    writer: 该组件给下游信息的 Writer，只需要一个，因为支持多个下游订阅
    """

    seq: int = TickDataDescriptor()
    valid: bool = TickDataDescriptor()
    extra_data: dict = TickDataDescriptor()
    resource_df: pd.DataFrame = TickDataDescriptor()
    task_df: pd.DataFrame = TickDataDescriptor()
    user_df: pd.DataFrame = TickDataDescriptor()
    metrics: dict = TickDataDescriptor()

    def __init__(self, *, name: str, conn: ProcessConnection, global_config_conn: ProcessConnection):
        self.name = name
        self.upstreams: Dict[str, ProcessConnection] = {}
        self.__conn = conn
        self.tick_data: TickData = TickData()
        # 记录上游的 seq
        self.__upstream_seqs = {}
        # 由 monitor 来获取 / 设置的 global config
        self.global_config = {}
        self.__registered_global_config = {}
        self.__global_config_conn = global_config_conn
        self.__last_perf_counter = -1
        self.__last_perf_counter_list = defaultdict(list)
        self.__last_write_result = -1

    def _log(self, log_func, *args, **kwargs):
        with logger.contextualize(uuid=f'{self.name}#{self.seq}'):
            getattr(logger, log_func)(*args, **kwargs)

    # 这样写，代码提示就能看到了
    debug = functools.partialmethod(_log, 'debug')
    info = functools.partialmethod(_log, 'info')
    warning = functools.partialmethod(_log, 'warning')
    error = functools.partialmethod(_log, 'error')
    f_debug = functools.partialmethod(_log, 'f_debug')
    f_info = functools.partialmethod(_log, 'f_info')
    f_warning = functools.partialmethod(_log, 'f_warning')
    f_error = functools.partialmethod(_log, 'f_error')

    def add_upstream(self, name, conn: ProcessConnection):
        if self.upstreams.get(name):
            raise Exception('不能设置相同名字的 upstream')
        self.upstreams[name] = conn
        self.__upstream_seqs[name] = 0

    def register_global_config(self, **default_global_config):
        for global_config_key, default_global_config_value in default_global_config.items():
            assert isinstance(global_config_key, str), 'global_config_key 必须是字符串'
            # 放在这里校验 default_global_config_value 是 json serializable 的
            ujson.dumps(default_global_config_value)
            self.__registered_global_config[global_config_key] = default_global_config_value

    def get_upstream_data(self, upstream='default') -> TickData:
        """
        直接获取 upstream 的信息，默认为 default
        """
        return self.upstreams[upstream].get()

    def waiting_for_upstream_data(self, upstream='default') -> TickData:
        """
        阻塞式调用，等待并返回某个上游的下一次数据，不一定正好是下一次，但一定比现在新
        默认为 default
        """
        while True:
            if self.upstreams[upstream].header.seq > self.__upstream_seqs[upstream]:
                upstream_data = self.upstreams[upstream].get()
                self.__upstream_seqs[upstream] = upstream_data.seq
                return upstream_data
            time.sleep(0.001)

    def set_tick_data(self, tick_data=None):
        """
        把自己的 tick_data 设置成传入的 tick_data
        """
        self.tick_data = TickData() if tick_data is None else tick_data
        self.metrics = {}  # 这里有上游的 metrics

    def start(self):
        while True:
            try:
                self.tick_process()
            except Exception as e:
                logger.exception(e)
                self.error('出错了，等 2 秒')
                time.sleep(2)

    def tick_process(self):
        if K8sPreStopHook.receive_stop_pod():
            self.warning('收到了停止 scheduler 的指令，不继续运行了')
            # 为了等待当前的 match 结束，不然有可能出现写了数据库却没发出去信号的情况
            time.sleep(5)
            os.system("""ps -ef | grep -v PID | awk '{system("kill -KILL " $2)}'""")
        # 一开始默认是有效的，如果没有 load 成功 config 就认为无效，之后交给用户处理
        self.valid = True
        if not self.__load_global_config():
            self.valid = False
        self.user_tick_process()
        self.__write_result()

    def user_tick_process(self):
        raise NotImplementedError

    def update_metric(self, name: str, value: float, log=False):
        """
        提供了一个上报 metric 的接口，由 monitor 来集中处理
        """
        self.metrics[name] = value
        self.debug(f'update metric {name} {value}')

    def __write_result(self):
        """
        写入数据
        """
        self.tick_data.extra_data['registered_global_config'] = self.__registered_global_config
        if self.__last_write_result > 0:
            self.update_metric('write_result', self.__last_write_result)
        self.perf_counter()
        self.__conn.put(self.tick_data, seq=self.seq)
        self.__last_write_result = self.perf_counter()

    def __load_global_config(self):
        """
        拿到自己注册的所有 global_config，才算获取成功
        """
        global_config = self.__global_config_conn.get()
        flag = True
        for global_config_key in self.__registered_global_config:
            if global_config.get(global_config_key) is not None:
                self.global_config[global_config_key] = global_config[global_config_key]
            else:
                flag = False
        return flag

    def perf_counter(self, kind='last', keep=1, comp=0):
        """
        返回距离上次调用经过的 ms 时间，简化 export 性能 metric
        keep 为在当前调用位置保留几次数据
        case kind:
            last: 返回调用位置 perf 的最后一次数据（即本次调用的数据）
            avg: 返回调用位置最近 keep 次数的平均值
            max: 返回调用位置最近 keep 次数的最大值
            gt_counter: 返回调用位置最近 keep 次数大于 comp 的次数
            lt_counter: 返回调用位置最近 keep 次数小于 comp 的次数
        """
        new_counter = time.perf_counter()
        calframe = inspect.getouterframes(inspect.currentframe(), 2)[1]
        caller = (calframe.filename, calframe.function, calframe.lineno)
        if self.__last_perf_counter > 0:
            self.__last_perf_counter_list[caller].append((new_counter - self.__last_perf_counter) * 1000)
        self.__last_perf_counter_list[caller] = self.__last_perf_counter_list[caller][-keep:]
        self.__last_perf_counter = new_counter
        if len(self.__last_perf_counter_list[caller]) == 0:
            return -1
        if kind == 'last':
            return self.__last_perf_counter_list[caller][-1]
        if kind == 'avg':
            return sum(self.__last_perf_counter_list[caller]) / len(self.__last_perf_counter_list[caller])
        if kind == 'max':
            return max(self.__last_perf_counter_list[caller])
        if kind == 'gt_counter':
            return len([p for p in self.__last_perf_counter_list[caller] if p > comp])
        if kind == 'lt_counter':
            return len([p for p in self.__last_perf_counter_list[caller] if p < comp])
