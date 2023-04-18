import json
import os
import time

import munch
from kubernetes import client
from kubernetes.client.rest import ApiException

from conf import CONF
from base_model.training_task import TrainingTask
from conf.flags import STOP_CODE, QUE_STATUS, VALIDATION_TASK_FLAG, TASK_TYPE, EXP_STATUS
from db import redis_conn, MarsDB
from experiment_manager.manager.manager_utils import waiting_exit, constantly_brpop, kill_all_manager_process, get_log_uuid
from logm import logger, log_stage, bind_logger_task
from roman_parliament import register_archive, set_mass_info, register_parliament, cancel_archive, withdraw_parliament
from roman_parliament.utils import generate_key
from server_model.auto_task_impl import AutoTaskSchemaWithDbImpl
from server_model.selector import TrainingTaskSelector
from server_model.task_impl import DbOperationImpl
from server_model.user_data import initialize_user_data_roaming
from k8s import get_corev1_api, get_appsv1_api
from manager_utils import monitor_brpop


key = os.path.basename(__file__)
log_id = get_log_uuid(key)
with logger.contextualize(uuid=f'{log_id}.init'):
    process_start_time = time.time()
    task_id = int(os.environ['TASK_ID'])
    initialize_user_data_roaming(overwrite_enable_roaming=False)
    set_mass_info(key_list=[generate_key(class_name=TrainingTask.__name__, sign='id', value=task_id)], mass_name=f'{task_id}_{key}')
    register_parliament()
    task = TrainingTaskSelector.find_one_by_id(AutoTaskSchemaWithDbImpl, id=task_id)
    bind_logger_task(task)
    register_archive(task, sign='id')
    corev1_api = get_corev1_api()
    appsv1_api = get_appsv1_api()
    stop_nodes_already_called = False


s_code = STOP_CODE()

pod_delete_option = client.V1DeleteOptions(
    api_version='v1',
    grace_period_seconds=CONF.manager.delete_pod.grace_period_seconds,
    propagation_policy='Background'
)

with logger.contextualize(uuid=f'{log_id}.enter_exit'):
    if task.queue_status == QUE_STATUS.FINISHED:  # 处理manager在写完finished出于某种原因没删除自己导致的僵尸manager
        logger.warning('检测到manager一启动任务已经finished')
        if all([pod.status in EXP_STATUS.FINISHED for pod in task.pods]):
            logger.warning('检测到所有pod均已结束，直接关闭manager')  # 任务正常结束
            redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}', json.dumps({'action': 'stop_manager'}))
            redis_conn.expire(f'{CONF.manager.stop_channel}:{task.id}', 5 * 60)
        else:
            logger.warning('检测到有pod还未结束，尝试去结束pod')  # pod残留
            redis_conn.set(f'ban:{task.user_name}:{task.nb_name}:{task.chain_id}', 1)
            redis_conn.lpush(
                f'{CONF.manager.stop_channel}:suspend:{task_id}',
                json.dumps({'stop_code': STOP_CODE.MANUAL_STOP})
            )


def get_stop_code():
    stop_code = 0
    recorded_stop_code = redis_conn.get(f'lifecycle:{task.id}:stop_code')
    if not recorded_stop_code:
        return 0
    for code in recorded_stop_code.decode().strip().split('\n'):
        stop_code |= int(code)
    return stop_code


@log_stage(log_id)
def delete_pod(pod_id):
    try:
        corev1_api.delete_namespaced_pod_with_retry(
            name=pod_id, namespace=CONF.launcher.task_namespace,
            body=pod_delete_option,
        )
        logger.info(f'删除pod {pod_id} grace_period_seconds={CONF.manager.delete_pod.grace_period_seconds}')  # 加到日志大盘里
    except ApiException as e:
        if e.status == 404:
            logger.debug(f'未找到pod_id为{pod_id}的节点，可能已删除')
        else:
            logger.exception(e)
            logger.error(f'无法删除pod_id为{pod_id}的节点，错误编号{e}')


@log_stage(log_id)
def stop_nodes():
    """
    manager 把 子node 全关掉
    @return:
    """
    global stop_nodes_already_called
    if stop_nodes_already_called:
        logger.info(f'之前已经关闭过，不作任何处理')
        return
    stop_nodes_already_called = True

    logger.debug(f'开始关闭任务节点')
    if CONF.manager.not_stop_node_for_test:
        logger.warning(f'为了测试，不关闭任务节点，请到时候人工关闭')
        return
    node_num = len(task.assigned_nodes)
    user_name = task.user_name

    for i in range(node_num):
        pod_id = f'{user_name.replace("_", "-")}-{task_id}-{i}'
        delete_pod(pod_id)
        logger.info(f'删除pod {pod_id}')  # 加到日志大盘里


@log_stage(log_id)
def stop_manager():
    redis_conn.lpush('finished_task_channel', task_id)
    user_name = task.user_name

    logger.info('注销群众和档案')  # 加进日志大盘
    # 注销群众
    for m in ['check_logs', 'check_resource_released', 'check_running', 'check_unschedulable', 'stop_func', 'suspend_func', 'init_manager', 'client_handler']:
        withdraw_parliament(mass_name=f'{task_id}_{m}.py')

    # 注销档案
    cancel_archive(archive=task, sign='id')

    logger.info('删除manager')  # 加进日志大盘
    manager_id = f'{user_name.replace("_", "-")}-{task_id}-manager'
    try:
        appsv1_api.delete_namespaced_stateful_set_with_retry(
            name=manager_id,
            namespace=CONF.launcher.task_namespace
        )
    except ApiException as e:
        if e.status == 404:
            logger.debug(f'未找到manager sts {manager_id}，可能已删除')
        else:
            logger.exception(e)
            logger.error(f'无法删除manager sts {manager_id}，错误编号{e}', fetion=True)


@log_stage(log_id)
def restart_exp():
    logger.info(f'开始检查重启')  # 加到日志大盘里

    if task.nb_name.endswith(VALIDATION_TASK_FLAG):
        logger.warning('是测试任务，不重启')
        return

    stop_code = get_stop_code()
    if stop_code < STOP_CODE.HOOK_RESTART:  # 遇到硬件坏了的时候，除非manual_stop，不然重启
        if not (stop_code & STOP_CODE.INTERRUPT) and not (stop_code & STOP_CODE.UNSCHEDULABLE):
            return
        # INTERRUPT，UNSCHEDULABLE 才需要重启

    if redis_conn.get(f'ban:{task.user_name}:{task.nb_name}:{task.chain_id}'):
        # 这个任务被用户打断和重启同时发生，只关闭不重启
        logger.info('强制刷新 stop_code 为 manual stop')
        redis_conn.append(f'lifecycle:{task.id}:stop_code', f'{STOP_CODE.MANUAL_STOP}\n')
        return

    if task.task_type == TASK_TYPE.JUPYTER_TASK and not task.user.is_internal and not task.group.startswith(CONF.jupyter.shared_node_group_prefix):
        logger.info('外部用户独占节点，不重启, 并刷新 stop_code 为 manual stop')
        redis_conn.append(f'lifecycle:{task.id}:stop_code', f'{STOP_CODE.MANUAL_STOP}\n')
        return

    logger.info('创建新任务')  # 加到日志大盘里
    log_msg = f'执行restart_exp({task.job_info})，优先级为{task.priority}，尝试重启任务，信号为 {bin(stop_code)}, 解析成: {s_code.name(stop_code)}'
    try:
        new_task = TrainingTaskSelector.find_one(DbOperationImpl, id=task_id)
        with MarsDB() as conn:
            task.queue_status = QUE_STATUS.FINISHED  # 告知k8sworker这个任务已经结束了
            task.update(('queue_status',), (QUE_STATUS.FINISHED,), db_conn=conn)
            new_task = new_task.resume(db_conn=conn)

        log_msg += ' -> 成功' if new_task else '失败'
        logger.warning(log_msg)
    except Exception as e:
        logger.exception(e)
        logger.error(f'{task.job_info} 重启的时候发生了未知错误: {e}')


with logger.contextualize(uuid=f'{log_id}.stop_loop'):
    while True:
        try:
            info = munch.Munch.fromJSON(constantly_brpop(f'{CONF.manager.stop_channel}:{task.id}')[1].decode())
            monitor_brpop(f'{CONF.manager.stop_channel}:{task.id}', info, process_start_time=process_start_time, module_name='stop_func')
            logger.debug(f'received {info}')
            if 'flag' in info:
                redis_conn.append(f'lifecycle:{task.id}:stop_code', f'{info.flag}\n')
            stop_code = get_stop_code()
            if info.action == 'stop_single_pod':  # pod complete的时候把它删掉
                delete_pod(info.pod_id)
            elif info.action == 'stop':  # stop node，以一个list的形式给出
                logger.info(
                    f'收到 stop 信号 {bin(info.flag)}, 解析成: {s_code.name(info.flag)}, '
                    f'合成 {bin(stop_code)} - {s_code.name(stop_code)}')
                if stop_code & STOP_CODE.MANUAL_FAILED:
                    for rank in range(task.nodes):
                        task.update_pod_status(rank, EXP_STATUS.FAILED)
                elif stop_code & STOP_CODE.MANUAL_SUCCEEDED:
                    for rank in range(task.nodes):
                        task.update_pod_status(rank, EXP_STATUS.SUCCEEDED)
                stop_nodes()
            else:  # stop manager
                redis_conn.set(f'manager_ban:{task_id}', b'1')
                logger.info(f'收到 stop_manager 信号 {bin(stop_code)}, '
                            f'manager 处理信号: {bin(stop_code)} - {s_code.name(stop_code)}')
                task.update(('stop_code',), (stop_code,))
                # stop manager
                restart_exp()
                task.queue_status = QUE_STATUS.FINISHED  # 告知k8sworker这个任务已经结束了
                task.update(('queue_status',), (QUE_STATUS.FINISHED,))
                # 根据 stop_code 更新 save_metric 终态
                failed_msg = redis_conn.get(f'lifecycle:{task_id}:failed_msg')
                task_event = redis_conn.get(f'lifecycle:{task_id}:task_event')
                if failed_msg or task_event:
                    if task.task_type == TASK_TYPE.VALIDATION_TASK:  # 如果是validation任务，得找到相对应的virtual任务才行
                        virtual_task = TrainingTaskSelector.find_one(AutoTaskSchemaWithDbImpl, chain_id=task.chain_id.split('_main_')[0])
                        virtual_task.create_error_info((failed_msg.decode() if failed_msg else "") + (task_event.decode() if task_event else ""))
                    else:
                        task.create_error_info((failed_msg.decode() if failed_msg else "") + (task_event.decode() if task_event else ""))

                # clear redis
                logger.info(f'清空redis')  # 加到日志大盘
                redis_conn.expire(f'disable_warn:{task_id}', 5 * 60)
                redis_conn.expire(f'watch_dog_time:{task.user_name}:{task_id}', 5 * 60)
                redis_conn.expire(f'exp_est_time:{task.user_name}:{task_id}', 5 * 60)
                redis_conn.expire(f'{CONF.manager.stop_channel}:{task_id}:update_status.py', 5 * 60)
                redis_conn.expire(f'{CONF.manager.stop_channel}:suspend:{task.id}', 5 * 60)
                redis_conn.expire(f'{CONF.manager.stop_channel}:{task_id}', 5 * 60)
                redis_conn.expire(f'{CONF.manager.redis_message_channel}', 5 * 60)
                redis_conn.expire(f'lifecycle:{task_id}:log_time', 5 * 60)
                redis_conn.expire(f'lifecycle:{task_id}:failed_msg', 5 * 60)
                redis_conn.expire(f'lifecycle:{task_id}:task_event', 5 * 60)
                redis_conn.expire(f'lifecycle:{task_id}:stop_code', 5 * 60)
                for module in ['check_logs.py', 'check_resource_released.py', 'check_running.py',
                               'check_unschedulable.py',
                               'init_manager.py', 'stop_func.py', 'suspend_func.py', 'update_status.py',
                               'client_responder.py', 'client_handler.py']:
                    redis_conn.expire(f'module:{task_id}:{module}', 5 * 60)

                logger.info(f'调用 stop_manager， 开始关闭 manager')
                stop_manager()
                break
        except Exception as e:
            logger.exception(e)
            logger.error(f'在做stop的时候出问题了，请尽快检查: {e}')
            kill_all_manager_process()

    waiting_exit()
