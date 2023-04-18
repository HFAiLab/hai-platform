import json
import os
import time

from conf import CONF
from base_model.training_task import TrainingTask
from conf.flags import EXP_STATUS
from db import redis_conn
from experiment_manager.manager.manager_utils import waiting_exit, get_log_uuid
from server_model.user_data import initialize_user_data_roaming
from k8s import get_custom_corev1_api

from logm import logger, log_stage, bind_logger_task
from roman_parliament import register_archive, set_mass_info, register_parliament
from roman_parliament.utils import generate_key
from server_model.auto_task_impl import AutoTaskSchemaImpl
from server_model.selector import TrainingTaskSelector

module = os.path.basename(__file__)
log_id = get_log_uuid(module)
with logger.contextualize(uuid=f'{log_id}.init'):
    task_id = int(os.environ['TASK_ID'])
    if redis_conn.get(f'manager_ban:{task_id}') == b'1':
        waiting_exit()
    custom_k8s_api = get_custom_corev1_api()
    k8s_namespace = CONF.launcher.task_namespace
    initialize_user_data_roaming(overwrite_enable_roaming=False)
    set_mass_info(key_list=[generate_key(class_name=TrainingTask.__name__, sign='id', value=task_id)], mass_name=f'{task_id}_{module}')
    register_parliament()
    task = TrainingTaskSelector.find_one_by_id(AutoTaskSchemaImpl, id=task_id)
    bind_logger_task(task)
    register_archive(task, sign='id')


@log_stage(log_id)
def check_resource_released():
    unreleased_pods = {pod.pod_id for pod in task.re_pods().pods if pod.status not in EXP_STATUS.FINISHED}
    pods_name = []
    try:
        pods_name = json.loads(redis_conn.get('active_pods_name').decode())
        redis_time = float(redis_conn.get('active_pods_time').decode())
        if time.time() - redis_time > 10:  # 缓存已超时
            redis_conn.set('active_pods_time', time.time())  # 让其它manager先用以前的cache，这个manager来负责更新
            raise Exception
        else:  # 缓存没有超时，可以继续拿缓存的
            if unreleased_pods & set(pods_name) != unreleased_pods:  # 缓存里存活的pod没有包含未释放的pod
                logger.debug(f'task {task_id}缓存里存活的pod没有包含未释放的pod')
    except Exception as e:
        try:
            k8s_pods = custom_k8s_api.list_namespaced_pod_with_retry(namespace=k8s_namespace, label_selector='compute_node=true,type!=manager', resource_version='0')
            pods_name = [k8s_pod['metadata']['name'] for k8s_pod in k8s_pods['items']]
            redis_conn.set('active_pods_time', time.time())
            redis_conn.set('active_pods_name', json.dumps(pods_name))
        except Exception as e:
            logger.exception(e)
            logger.f_error(f'manager因为{e}挂了，自杀manager，请系统组检查')
            os._exit(1)
    reported_pods = unreleased_pods - set(pods_name)

    for reported_pod in reported_pods:
        logger.info(f'发现{reported_pod}已经结束，该pod正式进入结束状态')  # 更改pod状态
        task.update_pod_status(rank=int(reported_pod.split('-')[-1]), status='terminated')
    if len(unreleased_pods) == 0:  # 所有pod都结束了
        logger.info('发送stop manager')  # 加进日志大盘里
        redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}', json.dumps({'action': 'stop_manager'}))
        redis_conn.expire(f'{CONF.manager.stop_channel}:{task.id}', 5 * 60)

    return False


while True:  # 现在主要靠k8s_celery_worker来做check_resource_released，这个是为了避免单点风险做的兜底
    time.sleep(10)
    if check_resource_released():
        break

waiting_exit()
