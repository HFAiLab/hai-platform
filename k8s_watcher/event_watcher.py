
import ujson
from dateutil.parser import parse
from datetime import datetime, timedelta, timezone

from .base import ListWatcher
from logm import logger, log_stage
from db import redis_conn
from .utils import v1, custom_v1, module


class EventListWatcher(ListWatcher):
    def __init__(self, namespace=None, label_selector=None, field_selector=None, process_interval=10):
        super().__init__('event', custom_v1.list_namespaced_event, v1.list_namespaced_event, namespace, label_selector, field_selector, process_interval)
        self.reported_pods = set()
        self.namespace = namespace

    @log_stage(f'{module}.eventwatcher')
    def process(self):
        task_messages = dict()
        for item in self._data.copy().values():
            # 只看节点上报的fail event
            if 'source' in item.keys() and item['source'].get('component', '') == 'kubelet' and 'fail' in item['message'].lower():
                if 'failed to delete \\"eth0\\": no such device' in item['message']:
                    # skip as this is normal warning
                    continue
                pod_name = item['involvedObject']['name']
                try:
                    task_id = pod_name.split('-')[-2]
                except:
                    continue
                if pod_name not in self.reported_pods:
                    self.log_info(f"pod_name: {pod_name}, event: {item['message']}")
                    self.reported_pods.add(pod_name)
                    resp = v1.read_namespaced_pod_with_retry(pod_name, self.namespace, _preload_content=False, _request_timeout=5)
                    if resp is None:
                        self.log_info(f"{pod_name} already deleted")
                        continue
                    pod = ujson.loads(resp.data)
                    # 过滤非任务容器
                    # 任务启动时间小于30分钟才记录到redis
                    if pod['metadata'].get('labels', {}).get('compute_node', '') == 'true' and \
                        datetime.utcnow().replace(tzinfo=timezone.utc) - \
                            parse(pod['metadata']['creationTimestamp']).astimezone(timezone.utc) \
                                <= timedelta(minutes=30):
                        if task_id not in task_messages.keys():
                            task_messages[task_id] = item['message']
                        else:
                            task_messages[task_id] += f"\n{item['message']}"
        for task_id, msg in task_messages.items():
            self.log_info(f'reported {task_id} error event: {msg}')
            redis_conn.set(f'lifecycle:{task_id}:task_event', f'有任务容器遇到错误，请及时联系管理员\n{msg}')
