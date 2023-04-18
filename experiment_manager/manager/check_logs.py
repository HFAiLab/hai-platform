import json
import os
import time
from threading import Thread

import zmq

from conf import CONF
from base_model.training_task import TrainingTask
from conf.flags import EXP_STATUS, STOP_CODE, WARN_TYPE, TASK_TYPE
from experiment_manager.manager.manager_utils import waiting_exit, get_disbale_warn, get_log_uuid
from logm import logger, log_stage, bind_logger_task
from roman_parliament import register_archive, set_mass_info, register_parliament
from roman_parliament.utils import generate_key
from server_model.auto_task_impl import AutoTaskSchemaImpl
from server_model.selector import TrainingTaskSelector
from server_model.task_runtime_config import TaskRuntimeConfig
from server_model.auto_task_impl import AutoTaskSchemaWithDbImpl
from server_model.user_data import initialize_user_data_roaming
from db import redis_conn

module = os.path.basename(__file__)
task_id = int(os.environ["TASK_ID"])
log_id = get_log_uuid(module)

with logger.contextualize(uuid=f'{log_id}.enter_exit'):
    if redis_conn.get(f'manager_ban:{task_id}') == b'1':
        waiting_exit()


@log_stage(log_id)
def setup():
    set_mass_info(key_list=[generate_key(class_name=TrainingTask.__name__, sign='id', value=task_id)], mass_name=f'{task_id}_{module}')
    initialize_user_data_roaming(overwrite_enable_roaming=False)
    register_parliament()
    task = TrainingTaskSelector.find_one_by_id(AutoTaskSchemaImpl, id=task_id)
    bind_logger_task(task)
    register_archive(task, sign='id')

    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    url = f"tcp://{task.user_name}-{task_id}-0:5775"
    socket.connect(url)
    logger.info(f'zmq socket {url} connected')
    return task, socket


def send_failure_msg(task, msg):
    task.re_impl(AutoTaskSchemaImpl)
    redis_conn.append(f'lifecycle:{task_id}:failed_msg', f'{msg}\n')
    for rank in range(len(task.pods)):
        task.update_pod_status(rank=rank, status=EXP_STATUS.FAILED)
    redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}', json.dumps({'action': 'stop', 'flag': STOP_CODE.TIMEOUT}))


@log_stage(log_id)
def handle_task_event(task: TrainingTask, socket):
    task.re_impl(AutoTaskSchemaWithDbImpl)
    while True:
        response = socket.recv().decode('utf-8')
        logger.info(f'收到消息 {response}')
        try:
            event = json.loads(response)
            if event['event_type'] == 'timeout':
                if task.task_type == TASK_TYPE.TRAINING_TASK:   # send fetion
                    send_fetion = not(bool(WARN_TYPE.LOG & get_disbale_warn()))
                    logger.f_warning(event['msg'], fetion=send_fetion, warn_type=WARN_TYPE.LOG)
                send_failure_msg(task, event['msg'])
                break
            elif event['event_type'] == 'service_status_change':
                service_name, alive = event['service_name'], event['alive']
                TaskRuntimeConfig(task).update_by_path(path=('services', service_name, 'alive'), value=alive, source='service_task')
        except Exception as e:
            logger.exception(e)
            logger.error(f'处理消息失败：{e}')


@log_stage(log_id)
def watch_service_control():
    send_socket = zmq.Context().socket(zmq.PUSH)
    send_socket.bind("tcp://*:5776")
    time.sleep(0.1)

    while True:
        data = redis_conn.brpop(f'manager_service_control:{task_id}')
        send_socket.send(data[1])


def check_log(task, socket):
    Thread(target=watch_service_control, daemon=True).start()
    handle_task_event(task, socket)


if __name__ == "__main__":
    task, socket = setup()
    check_log(task, socket)
    with logger.contextualize(uuid=f'{log_id}.finish_exit'):
        waiting_exit()
