import json
import time
import os

from conf import CONF, CONTAINER_NAME
from conf.flags import EXP_STATUS, STOP_CODE
from db import redis_conn
from experiment_manager.manager.manager_utils import waiting_exit, get_log_uuid
from server_model.selector import TrainingTaskSelector
from server_model.auto_task_impl import AutoTaskSchemaImpl
from k8s.podstate_utils import get_pod_state
from logm import logger, log_stage, bind_logger_task
from roman_parliament import register_archive, set_mass_info, register_parliament
from roman_parliament.utils import generate_key
from base_model.training_task import TrainingTask
from server_model.user_data import initialize_user_data_roaming
from k8s import get_custom_corev1_api


module = os.path.basename(__file__)
log_id = get_log_uuid(module)
with logger.contextualize(uuid=f'{log_id}.init'):
    task_id = int(os.environ['TASK_ID'])
    if redis_conn.get(f'manager_ban:{task_id}') == b'1':
        waiting_exit()
    initialize_user_data_roaming(overwrite_enable_roaming=False)
    set_mass_info(key_list=[generate_key(class_name=TrainingTask.__name__, sign='id', value=task_id)], mass_name=f'{task_id}_{module}')
    register_parliament()
    task = TrainingTaskSelector.find_one_by_id(AutoTaskSchemaImpl, id=task_id)
    k8s_namespace = task.user.config.task_namespace
    bind_logger_task(task)
    register_archive(task, sign='id')
    custom_k8s_api = get_custom_corev1_api()


@log_stage(log_id)
def check_unschedulable():
    try:
        logger.info('检查是否unschedulable')
        try:
            k8s_pods = custom_k8s_api.list_namespaced_pod_with_retry(namespace=k8s_namespace,
                                                          label_selector='task_id={}'.format(task_id),
                                                          resource_version='0')
        except Exception as e:
            logger.exception(e)
            logger.f_error(f'manager因为{e}挂了，自杀manager，请系统组检查')
            os._exit(1)
        is_unschedulable = False
        blocked_pods = []
        for k8s_pod in k8s_pods['items']:
            pod_state = get_pod_state(pod_dict=k8s_pod, container_names=[CONTAINER_NAME])
            job_status = pod_state['status']
            pod_id = pod_state['details']['pod_name']
            logger.debug(f'unschedulable检查：查询到pod_id为{pod_id}的节点状态为{job_status}')
            if job_status in [EXP_STATUS.CREATED, EXP_STATUS.BUILDING, EXP_STATUS.UNSCHEDULABLE]:
                is_unschedulable = True
                blocked_pods.append([pod for pod in task.pods if pod.pod_id == pod_id][0])
        if is_unschedulable:
            try:
                # 生成 msg 放在前面不然报警的时候会看不清楚
                alert_msg = f'{task.job_info} 状态为 unschedulable，超过 {timeout_Ms} 分钟, 出错节点：{[p.node for p in blocked_pods]}，已经将该任务结束'
                logger.error(f'{alert_msg}。将unschedulable的节点标记为failed并发送stop信号')
                # 先标记成failed再stop
                redis_conn.append(f'lifecycle:{task_id}:failed_msg', f'{alert_msg}\n')
                for pod in blocked_pods:
                    task.update_pod_status(rank=int(pod.job_id), status=EXP_STATUS.FAILED)
                logger.f_error(f'检查失败，有处于 UNSCHEDULABLE 的节点 ({[p.node for p in blocked_pods]})，将重启训练')
                redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}', json.dumps({'action': 'stop', 'flag': STOP_CODE.UNSCHEDULABLE}))
            except Exception as e:
                logger.exception(e)
                logger.f_error(f'出错: {e}')
        else:
            logger.debug(f'检查通过没有处于 UNSCHEDULABLE 的节点')

    except Exception as e:
        logger.exception(e)
        logger.error(f'检查unschedulable的时候发生错误，错误编号: {e}')  # 防止在检查时pod不存在，兜底

    waiting_exit()


timeout_Ms = float(CONF.manager.get('unschedulable_timeout_Ms', 3))
time.sleep(60 * timeout_Ms)
check_unschedulable()
