

import os
import time
import ujson
import psutil
import datetime

from conf import CONF
from db import MarsDB
from .base_processor import BaseProcessor
from .connection import ProcessConnection


class Monitor(BaseProcessor):
    """
    监控模块，每秒都会看进程是否死了，且每隔用户设置的 interval 会检查是否有数据产出
    另外会收集各个模块上报的 metrics
    """

    def __init__(
            self,
            *,
            check_interval: int,
            scheduler_modules: dict,
            global_config_conn: ProcessConnection,
            **kwargs
    ):
        super(Monitor, self).__init__(global_config_conn=global_config_conn, **kwargs)
        self.upstream_seqs = {}
        self.scheduler_modules = scheduler_modules
        self.global_config_conn = global_config_conn
        self.check_interval = check_interval
        self.tick_metrics = {}
        for name, module in scheduler_modules.items():
            self.add_upstream(name, module['conn'])
            self.upstream_seqs[name] = -1

    def set_global_config(self, global_config_key, global_config_value):
        """
        规定只能通过 api 接口调用这个方法 set global config，直接改 redis 不生效
        """
        self.global_config[global_config_key] = global_config_value
        MarsDB().execute('''
        insert into "multi_server_config" ("key", "value", "module")
        values 
        (%s, %s, %s)
        on conflict ("key", "module") do update set "value" = excluded."value"
        ''', (global_config_key, ujson.dumps(global_config_value), 'scheduler'))
        self.global_config_conn.put(self.global_config)

    def user_tick_process(self):
        # 每个整秒进行监控
        ts = datetime.datetime.now().timestamp()
        self.seq = 1000 * (int(ts) + 1)
        time.sleep(int(ts) + 1 - ts)
        tick_metrics = {}
        for name, module_config in self.scheduler_modules.items():
            if not module_config['process'].is_alive():
                self.f_error(f'{name} 进程死了，尝试重启')
                # 这个重启不安全，得全杀了，得设计一个机制
                # subscriber 也许可以简单重启
                os.system("""ps -ef | grep -v PID | awk '{system("kill -KILL " $2)}'""")
            tick_metrics[f'{name},process_rss'] = psutil.Process(module_config['process'].pid).memory_info().rss
            tick_data = self.get_upstream_data(name)
            if self.seq % self.check_interval == 0:
                if tick_data.seq == self.upstream_seqs[name]:
                    log_str = f"scheduler 异常 {name} 已经 {self.check_interval} ms 没有产出数据了，请人工检查"
                    self.f_error(log_str)
                self.upstream_seqs[name] = tick_data.seq
            tick_metrics = {**{
                f'{name},{k}': v for k, v in tick_data.metrics.items()
            }, **tick_metrics}
            registered_global_config = tick_data.extra_data.get('registered_global_config', {})
            for k, v in registered_global_config.items():
                # 如果还没有这项全局配置，就加上，赋予默认值
                if self.global_config.get(k) is None:
                    self.set_global_config(k, v)
        self.tick_metrics = tick_metrics
