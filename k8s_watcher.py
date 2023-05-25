import os
import uuid
import threading

from kubernetes.leaderelection.resourcelock.configmaplock import ConfigMapLock

from conf import CONF
from conf.flags import QUE_STATUS, TASK_TYPE
from roman_parliament import register_parliament, register_archive
from roman_parliament.archive_triggers import add_archive_trigger
from roman_parliament.archive_triggers.training_task_trigger import TrainingTaskTrigger
from server_model.auto_task_impl import AutoTaskSchemaImpl
from server_model.selector import TrainingTaskSelector
from k8s import LeaderElection, LeaderElectionConfig
from logm import logger, log_stage
from k8s_watcher.utils import module
from k8s_watcher import PodListWatcher, NodeListWatcher, EventListWatcher


def init_parliament():
    add_archive_trigger(TrainingTaskTrigger)
    # 获取当前正在运行的所有档案并记录
    training_tasks = TrainingTaskSelector.where(
        AutoTaskSchemaImpl, f'''
        "queue_status"=%s AND 
        "task_type" in ('{TASK_TYPE.JUPYTER_TASK}', '{TASK_TYPE.TRAINING_TASK}', '{TASK_TYPE.VALIDATION_TASK}')
        ''',
        (QUE_STATUS.SCHEDULED,),
        limit=10000
    )
    for training_task in training_tasks:
        register_archive(training_task, sign='id')
    register_parliament()  # 等archive都到齐了，再去订阅信号


node_list_watcher = NodeListWatcher(process_interval=10)
pod_list_watchers, event_list_watchers = {}, {}
for namespace in CONF.launcher.task_namespaces_by_role.values():
    pod_list_watchers[namespace] = PodListWatcher(namespace, process_interval=1)
    event_list_watchers[namespace] = EventListWatcher(namespace, field_selector='type=Warning', process_interval=10)


healthy = True


def thread_excepthook(args):
    global healthy
    logger.exception(args.exc_value)
    logger.error(f'Exception in thread {args.thread}.')
    healthy = False


@log_stage(module)
def run():
    logger.info("Start leading")
    init_parliament()
    threading.Thread(name='node_list_watcher', target=node_list_watcher.run).start()
    for key, pod_list_watcher in pod_list_watchers.items():
        threading.Thread(name=f'pod_list_watcher_{key}', target=pod_list_watcher.run).start()
    for key, event_list_watcher in event_list_watchers.items():
        threading.Thread(name=f'event_list_watcher_{key}', target=event_list_watcher.run).start()


@log_stage(module)
def stop_and_die():
    logger.error("Stop leading")
    node_list_watcher.stop()
    for watcher in pod_list_watchers.values():
        watcher.stop()
    for watcher in event_list_watchers.values():
        watcher.stop()
    os._exit(1)


if __name__ == '__main__':
    threading.excepthook = thread_excepthook    # override excepthook
    identity = f'{module}_{uuid.uuid4()}'
    lock_name = CONF.k8swatcher.configmap_lock
    logger.info(f'leader identity: {identity}, lock: {lock_name}')
    default_namespace = CONF.launcher.task_namespaces_by_role['internal']
    config = LeaderElectionConfig(ConfigMapLock(lock_name, os.environ.get('NAMESPACE', default_namespace), identity),
                                  lease_duration=15,
                                  renew_deadline=10,
                                  retry_period=5,
                                  onstarted_leading=run,
                                  onstopped_leading=stop_and_die,
                                  keep_leading=lambda: healthy)

    # Enter leader election
    LeaderElection(config).run()
