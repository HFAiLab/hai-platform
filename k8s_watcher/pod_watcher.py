import os
import pickle
import ujson
import time

from .base import ListWatcher
from logm import log_stage
from base_model.training_task import TrainingTask
from server_model.auto_task_impl import AutoTaskSchemaImpl
from server_model.pod import Pod
from roman_parliament import archive_dict
from db import redis_conn
from .utils import all_corev1, all_custom_corev1
from k8s.podstate_utils import get_pod_state

module = os.environ.get('POD_NAME', 'k8swatcher-0')


class PodListWatcher(ListWatcher):
    def __init__(self, namespaces=None, label_selector=None, field_selector=None, process_interval=10):
        list_watch_funcs = {
            host: (all_custom_corev1[host].list_namespaced_pod, all_corev1[host].list_namespaced_pod)
            for host in all_custom_corev1.keys()
        }
        super().__init__('pod', list_watch_funcs, namespaces, label_selector, field_selector, process_interval)
        self.old_pods_namelist = set()
        self.last_redis_update_time = time.time()
        self.last_task_set = set()
        self.current_task_list = []
        self.count = 0
        # log forest watcher
        self.last_result = {}
        # record pod exit status
        self.recorded_exit_pod = set()

    def _update_pods(self):
        keys = list(filter(lambda x: TrainingTask.__name__ in x, archive_dict.keys()))
        if self.count % 1000 == 0:
            self.log_info(f'运行了 [{self.count + 1}] 次 当前存活id长度: [{len(keys)}]')

        all_task_info = []
        removed_pod_ids = []
        for key in keys:
            try:  # 可能在枚举的时候archive_dict被更新了
                task = archive_dict[key]
                task.re_impl(AutoTaskSchemaImpl)
                all_task_info.append(f'{task.user_name}_{task.id}')
                for reported_pods in [pod for pod in task.pods if pod.pod_id not in self.current_task_list and pod.status.endswith('TERMINATING')]:
                    task.update_pod_status(rank=int(reported_pods.job_id), status='terminated')
                    removed_pod_ids.append(reported_pods.pod_id)
            except:
                pass
        # 打印任务变动细节
        task_set = set(all_task_info)
        add_task_set = task_set - self.last_task_set
        remove_task_set = self.last_task_set - task_set
        for sk, info in zip([add_task_set, remove_task_set], ['新增', '删除']):
            if (lsk := len(sk)) > 0:
                self.log_info(f'当前 {info} id: 长度 [{lsk}]，详情 {sk}')
        self.last_task_set = task_set
        # 打印删除的细节
        if (len_rm_keys := len(removed_pod_ids)) > 0:
            self.log_info(f'本次通知删除id len: [{len_rm_keys}], ids: {sorted(removed_pod_ids)}')
        self.count += 1

    def process(self):
        self._data_copied = {k: v.copy() for k, v in self._data.items()}
        self.process_pod_update()
        self.process_pod_exit()
        self.process_logforest()

    @log_stage(module)
    def process_pod_update(self):
        # task pods has label compute_node=true
        self.current_task_list = [
            k for data in self._data_copied.values() for k, v in data.items()
            if v['metadata'].get('labels', {}).get('compute_node', '') == 'true'
        ]

        # 只有pod变化时，才需要触发update_pods
        self._update_pods()
        # 更新redis
        # 注：这里的运行间隔不需要像update_pods那么频繁，故增加5s的延迟
        if time.time() - self.last_redis_update_time > 5:
            self.last_redis_update_time = time.time()
            redis_conn.set('active_pods_time', time.time())
            new_pods_namelist = set(self.current_task_list)
            if self.old_pods_namelist == new_pods_namelist:
                return
            redis_conn.set('active_pods_name', ujson.dumps(self.current_task_list))
            self.old_pods_namelist = new_pods_namelist

    @log_stage(module)
    def process_logforest(self):
        result = {
            pod_id: {
                'status': get_pod_state(pod_dict=event)['status'],
                'start_time': None
            }
            for data in self._data_copied.values() for pod_id, event in data.items()
        }
        pod_id_list = list(result.keys())
        for pod_id in pod_id_list:
            if pod_id in self.last_result and self.last_result[pod_id]['status'] == result[pod_id]['status']:
                result[pod_id]['start_time'] = self.last_result[pod_id]['start_time']
            else:  # 全新的 pod_id 或者是全新的 status
                result[pod_id]['start_time'] = time.time()
        self.last_result = result
        redis_conn.set('log_forest_watcher_pod_status', pickle.dumps(result))

    @log_stage(module)
    def process_pod_exit(self):
        for data in self._data_copied.values():
            for k, v in data.items():
                if v['metadata'].get('labels', {}).get('compute_node', '') == 'true' and k not in self.recorded_exit_pod:
                    try:
                        exit_code = v['status']['containerStatuses'][0]['state']['terminated']['exitCode']
                        Pod.find_pods_by_pod_id(k)[0].update(('exit_code',), (str(exit_code),))
                        self.recorded_exit_pod.add(k)
                        self.log_info(f'更新pod {k} 退出码 {exit_code}')
                    except:
                        pass
