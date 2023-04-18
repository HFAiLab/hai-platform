import os
import threading
import time
import ujson
import munch
import zmq

from conf import CONF
from base_model.training_task import TrainingTask
from conf.flags import STOP_CODE, SUSPEND_CODE, TASK_FLAG
from db import redis_conn
from experiment_manager.manager.manager_utils import waiting_exit, constantly_brpop, get_log_uuid
from logm import logger, bind_logger_task
from roman_parliament import register_archive, register_parliament, set_mass_info
from roman_parliament.utils import generate_key
from server_model.auto_task_impl import AutoTaskSchemaImpl
from server_model.selector import TrainingTaskSelector, BaseTaskSelector
from server_model.user_data import initialize_user_data_roaming
from manager_utils import monitor_brpop

module = os.path.basename(__file__)
log_id = get_log_uuid(module)
with logger.contextualize(uuid=f'{log_id}.init'):
    process_start_time = time.time()
    task_id = int(os.environ['TASK_ID'])
    if redis_conn.get(f'manager_ban:{task_id}') == b'1':
        waiting_exit()
    initialize_user_data_roaming(overwrite_enable_roaming=False)
    set_mass_info(key_list=[generate_key(class_name=TrainingTask.__name__, sign='id', value=task_id)], mass_name=f'{task_id}_{module}')
    register_parliament()
    task = TrainingTaskSelector.find_one_by_id(AutoTaskSchemaImpl, id=task_id)
    bind_logger_task(task)
    register_archive(task, sign='id')

with logger.contextualize(uuid=f'{log_id}.create_zmq'):
    try:
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://{task.user_name}-{task_id}-0:5778")
    except Exception as e:
        logger.exception(e)
        logger.f_error('creating suspend zmq error!')


def recv_ignore_err():
    zmq_thread = threading.Thread(target=socket.recv)
    zmq_thread.start()
    start_time = time.time()
    while time.time() - start_time < 3:
        if not zmq_thread.is_alive():
            break
        time.sleep(0.1)


stop_type = STOP_CODE.STOP
stop_nodes_already_called = False

with logger.contextualize(uuid=f'{log_id}.waiting_signal'):
    try:
        # 任务挂起之前主训练进程回收到一个挂起的命令，过 5 s 任务没退出就强制挂断
        info = munch.Munch.fromJSON(constantly_brpop(f'{CONF.manager.stop_channel}:suspend:{task.id}')[1].decode())
        monitor_brpop(f'{CONF.manager.stop_channel}:suspend:{task.id}', info, process_start_time=process_start_time, module_name='suspend_func')
        redis_conn.append(f'lifecycle:{task.id}:stop_code', f'{info.stop_code}\n')
        logger.info('告知用户任务被打断')  # 这里要进日志大盘
        socket.send_string('set_suspend_flag')
        recv_ignore_err()
        # 如果任务没有响应，过x秒任务会被强制挂起。
        sr_waiting_seconds = int(CONF.manager.suspend_waiting_seconds.recieved)
        time.sleep(sr_waiting_seconds)
        try:
            socket.send_string('destroy_suspend_flag')  # 销毁掉，这样的话，在我们打断的时候，用户就不会读到 flag 了
            recv_ignore_err()
        except:
            # 用户在看到 flag 的时候，可能会直接自杀，这样 socket 就没用了
            logger.warning('可能已经被用户 go suspend 掉了')
            pass
        task = BaseTaskSelector.find_one(AutoTaskSchemaImpl, id=task_id)
        if task.suspend_code & TASK_FLAG.SUSPEND_CODE < SUSPEND_CODE.SUSPEND_RECEIVED:
            msg = f'任务 {sr_waiting_seconds}s 没有响应，我把它挂起'
            redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}',
                             ujson.dumps({'action': 'stop', 'flag': info.stop_code}))
        else:
            msg = f'任务 {sr_waiting_seconds}s 响应，知道将被挂起， code={task.suspend_code & TASK_FLAG.SUSPEND_CODE} '
        logger.info(msg)  # 收到用户反馈或者没收到用户反馈

        # 这边等一段时间，无论用户是否优雅保存，都直接杀掉
        sf_waiting_seconds = int(CONF.manager.suspend_waiting_seconds.final)
        time.sleep(sf_waiting_seconds)
        task = BaseTaskSelector.find_one(AutoTaskSchemaImpl, id=task_id)  # 获取现在的suspend_code
        if task.suspend_code & TASK_FLAG.SUSPEND_CODE == SUSPEND_CODE.SUSPEND_RECEIVED:  # 只收到，没go suspend
            logger.info(f'{task.user_name}的任务{task.job_info}在收到suspend_command指令后未正确go_suspend，请检查代码，挂起原因：{msg}')
        redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}',
                         ujson.dumps({'action': 'stop', 'flag': info.stop_code}))
        logger.info(f'等任务 {sr_waiting_seconds}s 后主动挂起')  # 强制关闭任务

        waiting_exit()

    except Exception as e:
        logger.exception(e)
        logger.error(f'在做suspend的时候出问题了，请尽快检查: {e}')
