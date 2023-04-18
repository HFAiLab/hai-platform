import json
import time
import zmq
import threading
import os
from urllib3.exceptions import ReadTimeoutError

from conf import CONF, CONTAINER_NAME
from conf.flags import EXP_STATUS, STOP_CODE
from db import redis_conn
from server_model.user_data import initialize_user_data_roaming
from k8s import get_corev1_api, get_custom_corev1_api
from experiment_manager.manager.manager_utils import waiting_exit, get_log_uuid
from server_model.selector import TrainingTaskSelector
from server_model.auto_task_impl import AutoTaskSchemaImpl
from k8s.watch import MyWatch
from k8s.podstate_utils import get_pod_state, PodStateException
from logm import logger, log_stage, bind_logger_task
from roman_parliament import register_archive, set_mass_info, register_parliament
from roman_parliament.utils import generate_key
from base_model.training_task import TrainingTask


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
    bind_logger_task(task)
    register_archive(task, sign='id')
    total_num = len(task.assigned_nodes)
    finished_num = len([pod for pod in task.pods if pod.status in EXP_STATUS.FINISHED])
    k8s_api = get_corev1_api()
    custom_k8s_api = get_custom_corev1_api()

with logger.contextualize(uuid=f'{log_id}.enter_exit'):
    if finished_num == total_num:
        logger.warning(f'一起{__file__}就发现所有pod都在终态，不再检查 running 了', uuid=f'{module}.start_finished')  # 启动时就已经到达终态
        waiting_exit()


with logger.contextualize(uuid=f'{log_id}.create_zmq'):
    try:
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://{task.user_name}-{task_id}-0:5779")
    except Exception as e:
        logger.exception(e)
        logger.f_error('creating suspend zmq error!')


def get_terminated_critical_sidecars(pod_state):
    terminated_sidecars = [
        (c, c_info['state']['terminated']['exitCode'])
        for c, c_info in pod_state['details']['container_statuses'].items() if
        c.endswith('-critical') and c_info['state'].get('terminated') is not None
    ]
    return terminated_sidecars


@log_stage(log_id)
def check_pod_disappeared(finished_pod_ids):
    """
    检查是否有pod已经退出了，如果有，则标记该任务failed
    :return:
    """
    is_failed_or_stopped = False
    pod_ids = {f'{task.user_name.replace("_", "-")}-{task_id}-{i}' for i in range(total_num)}
    k8s_pods = custom_k8s_api.list_namespaced_pod_with_retry(namespace=CONF.launcher.task_namespace,
                                                             label_selector=f'task_id={task_id},type!=manager',
                                                             resource_version='0')
    resource_version = k8s_pods['metadata']['resourceVersion']
    watched_pod_ids = set()
    for k8s_pod in k8s_pods['items']:
        pod_state = get_pod_state(pod_dict=k8s_pod, container_names=[CONTAINER_NAME])
        terminated_sidecars = get_terminated_critical_sidecars(pod_state)
        job_status = pod_state['status']
        pod_id = pod_state['details']['pod_name']
        watched_pod_ids.add(pod_id)
        if len(terminated_sidecars):
            logger.info(f'查询到pod_id为{pod_id}的 terminated_sidecars: {terminated_sidecars}，将任务标记为失败')
            job_status = EXP_STATUS.FAILED
        # 这边会刷数据库
        logger.info(f'查询到pod_id为{pod_id}的节点状态为{job_status}')
        task.update_pod_status(rank=int(pod_id.split('-')[-1]), status=job_status)
        if job_status in [EXP_STATUS.STOPPED, EXP_STATUS.FAILED]:
            finished_pod_ids.add(pod_id)
            is_failed_or_stopped = True
        if job_status == EXP_STATUS.SUCCEEDED:
            finished_pod_ids.add(pod_id)
            redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}', json.dumps({'action': 'stop_single_pod', 'pod_id': pod_id}))

    if len(finished_pod_ids) == len(pod_ids):  # 一开始就全部成功
        logger.warning(f'在一开始就发现所有节点都到了终态，强制关闭该任务并释放资源')
        redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}',
                            json.dumps({'action': 'stop', 'flag': STOP_CODE.STOP}))
        waiting_exit()

    logger.info(f'查询在manager启动前就stop了的pod, 已经finished的pod: {finished_pod_ids}')
    unwatched_pod_ids = pod_ids - watched_pod_ids - finished_pod_ids
    for unwatched_pod_id in unwatched_pod_ids:
        logger.info(f'pod_id为{unwatched_pod_id}的节点已经stop了')
        # 只标记非manager删除的pod为stopped
        task.update_pod_status(rank=int(unwatched_pod_id.split('-')[-1]), status=EXP_STATUS.STOPPED)
        is_failed_or_stopped = True

    if is_failed_or_stopped:
        logger.warning(f'在一开始就发现failed或者stopped或者没找到的节点，强制关闭该任务并释放资源')
        for pod_id in pod_ids:
            # 需要将所有任务标记为 EXP_STATUS.FINISHED
            if pod_id not in finished_pod_ids:
                task.update_pod_status(rank=int(pod_id.split('-')[-1]), status=EXP_STATUS.FAILED)
        redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}',
                            json.dumps({'action': 'stop', 'flag': STOP_CODE.INIT_FAILED}))
        # 直接退出
        waiting_exit()

    return resource_version, finished_pod_ids


@log_stage(log_id)
def check_timeout():
    """
    检查任务是不是超过了预设运行时间
    """
    if task.schema.get('options', {}).get('timeout', -1) > 0:
        rest_seconds = int(task.schema['options']['timeout'] - (time.time() - task.begin_at.timestamp()))  # 就不考虑打断的情况、动态更新 timeout 了
        if rest_seconds > 0:
            time.sleep(rest_seconds)
        logger.info(f'发现{task.id} 运行超时了（设定运行时间 {task.schema["options"]["timeout"]}s），发送stop信号尝试关闭该任务')
        redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}', json.dumps({'action': 'stop', 'flag': STOP_CODE.STOP}))


threading.Thread(target=check_timeout).start()
with logger.contextualize(uuid=f'{log_id}.monitor_loop'):
    pod_last_status = {}
    finished_pod_ids = set()
    stop_watcher = False
    while True:
        if stop_watcher:
            logger.info('到达终态，关闭watcher')
            break
        try:
            first_sleep = True
            w = MyWatch()
            # list，以获取resourceversion
            logger.info(f'开始list')
            resource_version, finished_pod_ids = check_pod_disappeared(finished_pod_ids)
            params = {
                'resource_version': resource_version,
                '_request_timeout': 1800, # 规避watcher卡住的问题
            }
            logger.info(f'开始watch状态变化，params: {params}')
            for event in w.stream(k8s_api.list_namespaced_pod,
                                  namespace=CONF.launcher.task_namespace,
                                  label_selector=f'task_id={task_id},type!=manager',
                                  **params):
                event_object = event['object']
                pod_state = get_pod_state(pod_dict=event_object, container_names=[CONTAINER_NAME])
                # 从pod_state中获取指标
                status = pod_state['status']
                message = pod_state['message']
                pod_id = pod_state['details']['pod_name']
                terminated_sidecars = get_terminated_critical_sidecars(pod_state)
                if len(terminated_sidecars) > 0:
                    status = EXP_STATUS.FAILED
                    logger.info(f'查询到pod_id为{pod_id}的 terminated_sidecars: {terminated_sidecars}，将任务标记为失败')
                logger.info(f'new event ---- status: {status}; pod_id: {pod_id}; message: {message}; terminated_sidecars: {terminated_sidecars}')  # 检测到状态
                task.update_pod_status(rank=int(pod_id.split('-')[-1]), status=status)
                if status in EXP_STATUS.FINISHED:
                    # 防止多次运行，因为 pod 在进入终态的时候还是会多次调用
                    if pod_id in pod_last_status:
                        continue
                    pod_last_status[pod_id] = status
                    finished_pod_ids.add(pod_id)
                    if len(finished_pod_ids) == total_num:
                        # 在多个节点的情况下，就是为 success 准备的
                        logger.info(f'发现所有节点都到了终态，任务容器应该在关闭了或者可以success退出了')
                        redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}', json.dumps({'action': 'stop', 'flag': STOP_CODE.STOP}))
                        stop_watcher = True

                if status in [EXP_STATUS.STOPPED, EXP_STATUS.FAILED]:
                    logger.info(f'发现{pod_id}处于{status}态，发送stop信号尝试关闭该任务')
                    # 这里我们先sleep 5秒再只结束节点，是为了等其它可能的fail态节点并被manager观察到
                    # 发送多次，这样 failed 的也会被发送出去, 如果看到 failed，可能其他节点也有 failed，可以等一下，如果是 stop，那么就是 stop 了
                    if status == EXP_STATUS.FAILED and first_sleep:
                        socket.send_string('worker_exited')
                        time.sleep(5)
                        first_sleep = False
                    flag = (STOP_CODE.FAILED if status == EXP_STATUS.FAILED else STOP_CODE.STOP)
                    redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}', json.dumps({'action': 'stop', 'flag': flag}))
                    stop_watcher = True

                if status in [EXP_STATUS.SUCCEEDED]:
                    redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}', json.dumps({'action': 'stop_single_pod', 'pod_id': pod_id}))

        except PodStateException as e:
            logger.info(f'ignored exception: {str(e)}')
        except ReadTimeoutError:
            # ignore read timeout caused by _request_timeout
            pass
        except Exception as e:
            logger.exception(e)
            logger.error(f'watch stream exception: {str(e)}')
        finally:
            w.stop()
            time.sleep(1)

    waiting_exit()
