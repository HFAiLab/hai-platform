from abc import ABC, abstractmethod
from datetime import datetime
from k8s.watch import MyWatch
import time
import threading
import schedule
from logm import logger, log_stage
import os
from .utils import module


class ListWatcher(ABC):
    def __init__(self, object_type, list_func, watch_func, namespace=None, label_selector=None, field_selector=None, process_interval=10):
        self.object_type = object_type
        self.list_func = list_func
        self.watch_func = watch_func
        self.watcher = MyWatch()
        self.namespace = namespace
        self.label_selector = label_selector
        self.field_selector = field_selector
        self.process_interval = process_interval
        # 保存实际数据，value为原始dict类型
        self._data = dict()
        # list cache是否初始化成功
        self._ready = False
        # 最近一次cache更新的时间
        self.last_update = None
        self._stop = False

    @log_stage(module)
    def list_watch(self):
        kwargs = {
            'label_selector': self.label_selector,
            'field_selector': self.field_selector,
            'resource_version': '0',
        }
        if self.namespace:
            kwargs['namespace'] = self.namespace
        kwargs['allow_watch_bookmarks'] = True
        # 临时的workaround，timeout之内没有收到新的event，会重启watch stream
        kwargs['_request_timeout'] = 300
        while True:
            try:
                logger.info(f'start {self.object_type} list with args {kwargs}')
                raw = self.list_func(**kwargs)
                latest_resource_version = raw['metadata']['resourceVersion']
                self._data = {item['metadata']['name']: item for item in raw['items']}
                self.last_update = datetime.now()
                self._ready = True
                # 为了保证stream重试，不需要添加timeout_seconds参数，且需指定resource_version
                kwargs['resource_version'] = latest_resource_version

                logger.info(f'start {self.object_type} watcher with args {kwargs}')
                for event in self.watcher.stream(self.watch_func, **kwargs):
                    try:
                        name = event['object']['metadata']['name']
                        if event['type'] == 'ADDED' or event['type'] == 'MODIFIED':
                            self._data[name] = event['object']
                        elif event['type'] == 'DELETED':
                            self._data.pop(name, None)
                        self.last_update = datetime.now()
                        # 调用到这里的时候，list cache肯定已经ready了
                        # 目前不需要event trigger process运行
                        # 如node: setlabel, cordon, uncordon操作产生的event，时效性都不强
                        # self.process()
                    except:
                        logger.debug(f'{self.object_type} bookmark event: {event}')
            except Exception as e:
                logger.error(f'{self.object_type} watcher exception: {str(e)}, last update time: {self.last_update.strftime("%Y-%m-%d %H:%M:%S")}!')
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
            if not self.list_watch_thread.is_alive():
                logger.error(f'{self.object_type} listwatch thread exited!')
                os._exit(1)
            if not self.schdule_thread.is_alive():
                logger.error(f'{self.object_type} period task thread exited!')
                os._exit(1)
            time.sleep(5)

    @log_stage(module)
    def run(self):
        # 运行listwatch
        self.list_watch_thread = threading.Thread(target=self.list_watch, name='list_watch', daemon=True)
        self.list_watch_thread.start()
        logger.info(f'started {self.object_type} listwatch thread')
        # 等待list cache ready
        while not self._ready:
            time.sleep(1)

        self.schdule_thread = threading.Thread(target=self._schedule, name='schedule', daemon=True)
        self.schdule_thread.start()
        logger.info(f'started {self.object_type} schedule thread')

        self.monitor_thread = threading.Thread(target=self._monitor, name='monitor', daemon=True)
        self.monitor_thread.start()
        logger.info(f'started {self.object_type} monitor thread')

    def stop(self):
        self.watcher.stop()
        self._stop = True
