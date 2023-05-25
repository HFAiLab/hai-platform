import asyncio
import json
import os
import time

from conf import CONF
from server_model.selector import BaseTaskSelector
from server_model.auto_task_impl import AutoTaskSchemaImpl
from logm import logger, bind_logger_task
from db import redis_conn


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

task_id = int(os.environ['TASK_ID'])
task = BaseTaskSelector.find_one(AutoTaskSchemaImpl, id=task_id)
bind_logger_task(task)
user_name = task.user_name
n_module = None


def setup_n_module(module):
    """
    记录 manager 的重启，返回 n_module 的数字；
    在第一次的时候调用，设置，manager 每次重启都 +1
    :return:
    """
    global n_module
    if n_module is not None:
        return n_module
    key = f'module:{task_id}:{module}'
    try:
        n_module = redis_conn.get(key)
        n_module = int(n_module) + 1 if n_module else 1
        redis_conn.set(key, n_module)
    except Exception as e:
        # redis 坏了，啥都没了，所以 raise 出来
        with logger.contextualize(uuid=f'#{task_id}#{module}#-1.enter_exit'):
            logger.exception(e)
            logger.error(e)
            raise
    return n_module


def get_log_uuid(module):
    return f'#{task_id}#{module}#{setup_n_module(module)}'


def waiting_exit():
    logger.info(f'waiting for stop container...')
    while True:
        time.sleep(1000)


def kill_all_manager_process():
    # 需要，重启 manager 相关的所有进程来处理异常，我们把他们杀掉，然后由 systemd 来处理
    os.system(
        "ps aux | grep  experiment_manager  | grep -v grep  | awk '{print $2}' | xargs -r kill")


def constantly_brpop(channel, timeout=300):  # 默认10分钟没订阅到就重新订阅
    data = None
    while data is None:
        data = redis_conn.brpop(channel, timeout=timeout)
    return data


def get_disbale_warn():
    disable_warn = redis_conn.get(f'disable_warn:{task_id}')
    return int(disable_warn.decode()) if disable_warn is not None else 0


def monitor_brpop(key, value, process_start_time=None, module_name='Unknown', time_delta_threshold=60):
    try:
        if isinstance(value, str):
            value = json.loads(value)
        if process_start_time is not None:
            value['hf_timestamp'] = max(value['hf_timestamp'], process_start_time)
        time_delta = int(time.time() - value['hf_timestamp'])
        logger.debug(f'{module_name}订阅的key: {key}得到结果{value}，耗时{time_delta}秒，阈值{time_delta_threshold}秒')
    except:
        pass
