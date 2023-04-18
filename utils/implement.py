import asyncio
import hashlib
import subprocess
import time
import json
import datetime
from functools import partial, wraps
from subprocess import STDOUT, check_output


# note 发现多进程调用这个，会产生一堆的 Z+ 子进程，没时间去研究这个(有可能是超时后没有取消任务。下面已尝试修复)
async def run_cmd_aio(cmd, timeout=3):
    from logm import logger
    read_task = None
    try:
        create = asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE,
                                                 stderr=asyncio.subprocess.PIPE)
        proc = await create
        read_task = asyncio.Task(proc.communicate())
        stdout, stderr = await asyncio.wait_for(read_task, timeout)  # timeout=3s
        if proc.returncode != 0:
            logger.error(' '.join([cmd, '->', 'Run Failed']))
            raise subprocess.CalledProcessError(proc.returncode, cmd, stdout,
                                                stderr)
    except asyncio.TimeoutError:
        logger.error(' '.join([cmd, '->', 'Run Timeout']))
        if read_task is not None:
            read_task.cancel()
        raise
    return stdout, stderr


def run_cmd_new(cmd, timeout=3):
    from logm import logger
    start_time = time.time()
    try:
        output = check_output(cmd, stderr=STDOUT, timeout=timeout, shell=True)
    except subprocess.CalledProcessError as e:
        time_elapsed = time.time() - start_time
        log_str = f'time_elapsed: {time_elapsed}, {cmd} -> Run Failed with exit code {e.returncode}, output: {e.output.decode()}'
        logger.error(log_str)
        raise Exception(log_str)
    except subprocess.TimeoutExpired:
        logger.error(' '.join([cmd, '->', 'Run Timeout']))
        raise
    # print(cmd, '->', 'Run Success')
    return output


def convert_task_job_to_key(task, rank: int):
    return f'{hashlib.sha256(f"{task.user_name}{task.nb_name}".encode("utf-8")).hexdigest()[0:50]}-{rank}'


def convert_to_external_node(node, prefix, rank):
    return f'hfai-{prefix}-{rank}'


def convert_to_external_task(task):
    task.assigned_nodes = [convert_to_external_node(n, 'rank', rank) for rank, n in enumerate(task.assigned_nodes)]
    for rank, pod in enumerate(task._pods_):
        pod.node = convert_to_external_node(pod.node, 'rank', rank)
    return task


def asyncwrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_running_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run


class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)

