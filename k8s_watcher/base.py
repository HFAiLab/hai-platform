import time
import threading
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional

from k8s.watch import MyWatch
import schedule
from logm import logger, log_stage
from .utils import module


class ListWatcher(ABC):
    def __init__(self, object_type, list_watch_funcs: Dict, namespaces: Optional[List] = None,
                 label_selector=None, field_selector=None, process_interval=10):
        self.object_type = object_type
        self.list_watch_funcs = list_watch_funcs
        self.namespaces = namespaces
        self.label_selector = label_selector
        self.field_selector = field_selector
        self.process_interval = process_interval

        self.list_watch_threads = dict()
        self.watchers = dict()
        # 保存实际数据，value为原始dict类型
        self._data = dict()
        # list cache是否初始化成功
        self._ready = dict()
        # 最近一次cache更新的时间
        self.last_update = dict()
        self._stop = False

    def log_info(self, info, index=None):
        if index is not None:
            info = f'[index={index}] ' + info
        logger.info(info)

    @log_stage(module)
    def list_watch(self, index, list_func, watch_func, namespace=None):
        kwargs = {
            'label_selector': self.label_selector,
            'field_selector': self.field_selector,
            'resource_version': '0',
        }
        if namespace:
            kwargs['namespace'] = namespace
        kwargs['allow_watch_bookmarks'] = True
        # 临时的workaround，timeout之内没有收到新的event，会重启watch stream
        kwargs['_request_timeout'] = 300
        self.watchers[index] = MyWatch()
        self._ready[index] = False
        while True:
            try:
                self.log_info(f'start {self.object_type} list with args {kwargs}', index)
                raw = list_func(**kwargs)
                latest_resource_version = raw['metadata']['resourceVersion']
                self._data[index] = {item['metadata']['name']: item for item in raw['items']}
                self.last_update[index] = datetime.now()
                self._ready[index] = True
                # 为了保证stream重试，不需要添加timeout_seconds参数，且需指定resource_version
                kwargs['resource_version'] = latest_resource_version

                self.log_info(f'start {self.object_type} watcher with args {kwargs}', index)
                for event in self.watchers[index].stream(watch_func, **kwargs):
                    try:
                        name = event['object']['metadata']['name']
                        if event['type'] == 'ADDED' or event['type'] == 'MODIFIED':
                            self._data[index][name] = event['object']
                        elif event['type'] == 'DELETED':
                            self._data[index].pop(name, None)
                        self.last_update[index] = datetime.now()
                        # 调用到这里的时候，list cache肯定已经ready了
                        # 目前不需要event trigger process运行
                        # 如node: setlabel, cordon, uncordon操作产生的event，时效性都不强
                        # self.process()
                    except:
                        logger.debug(f'{index} {self.object_type} bookmark event: {event}')
            except Exception as e:
                logger.error(f'{index} {self.object_type} watcher exception: {str(e)}, '
                             f'last update time: {self.last_update[index].strftime("%Y-%m-%d %H:%M:%S") if index in self.last_update.keys() else "None"}!')
            time.sleep(5)

    @abstractmethod
    def process(self):
        # process 运行有两种触发方式：定时任务，或者接收到k8s watch事件
        pass

    # 运行process
    @log_stage(module)
    def _schedule(self):
        schedule.every(self.process_interval).seconds.do(self.process)
        while True:
            if self._stop:
                break
            try:
                schedule.run_pending()
            except Exception as e:
                logger.exception(e)
                logger.error(f'run {self.object_type} period task failed: {e}')
                time.sleep(self.process_interval)
                continue
            time.sleep(0.5)

    @log_stage(module)
    def _monitor(self):
        while True:
            if self._stop:
                return
            for index, list_watch_thread in self.list_watch_threads.items():
                if not list_watch_thread.is_alive():
                    logger.error(f'{index} {self.object_type} listwatch thread exited!')
                    os._exit(1)
            if not self.schdule_thread.is_alive():
                logger.error(f'{self.object_type} period task thread exited!')
                os._exit(1)
            time.sleep(5)

    @log_stage(module)
    def run(self):
        # 运行listwatch
        for cluster_name, list_watch_func in self.list_watch_funcs.items():
            if self.namespaces is None:
                self.namespaces = [None]
            for namespace in self.namespaces:
                index = f'{cluster_name}:{namespace}' if namespace else cluster_name
                self.list_watch_threads[index] = threading.Thread(target=self.list_watch,
                                                                  name=f'list_watch-{index}',
                                                                  args=(index, *list_watch_func, namespace),
                                                                  daemon=True)
                self.list_watch_threads[index].start()
                self.log_info(f'started {self.object_type} listwatch thread', index)
        # 等待list cache ready
        while sum(self._ready.values()) != len(self.list_watch_threads):
            time.sleep(1)

        self.schdule_thread = threading.Thread(target=self._schedule, name='schedule', daemon=True)
        self.schdule_thread.start()
        self.log_info(f'started {self.object_type} schedule thread')

        self.monitor_thread = threading.Thread(target=self._monitor, name='monitor', daemon=True)
        self.monitor_thread.start()
        logger.info(f'started {self.object_type} monitor thread')

    def stop(self):
        for watcher in self.watchers.values():
            watcher.stop()
        self._stop = True
