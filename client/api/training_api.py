import os
import sys
import time
from typing import Optional
import sysv_ipc
import itertools
import pickle
import socket
from hfai.base_model.base_task import BaseTask
from hfai.conf.flags import EXP_PRIORITY, WARN_TYPE


WATCHDOG_TIME_SHM_ID = 237965198
try:
    WATCHDOG_TIME_SHM_ID_SHM = sysv_ipc.SharedMemory(WATCHDOG_TIME_SHM_ID, sysv_ipc.IPC_CREX, mode=0o777)
except sysv_ipc.ExistentialError:
    WATCHDOG_TIME_SHM_ID_SHM = sysv_ipc.SharedMemory(WATCHDOG_TIME_SHM_ID)

try:
    import pynvml
    no_pynvml = False
except:
    no_pynvml = True

# ================== 以下接口用于训练中调用 ======================================
IMPORT_TIME = time.time()
SIMULATE_SUSPEND_SEC = int(os.environ.get('SIMULATE_SUSPEND', -1))
IS_SIMULATE = os.environ.get('HFAI_SIMULATE', '0') == '1'


def nb_name() -> str:
    return os.environ.get("MARSV2_NB_NAME", "NO_CLUSTER")


def task_id() -> str:
    return os.environ.get("MARSV2_TASK_ID", -1)


def rank() -> int:
    return int(os.environ.get("MARSV2_RANK", 0))


def node_name() -> str:
    return os.environ.get('MARSV2_NODE_NAME', 'NAN_NODE')


def user_name() -> str:
    return os.environ.get('MARSV2_USER', '')


def current_selector_task() -> BaseTask:
    """ 获取调用 API 时用于选定任务的 task 实例, 仅有 nb_name 和 id 属性 """
    task = BaseTask()
    task.nb_name = nb_name()
    task.id = int(task_id())
    return task


def send_data(data, timeout: int = 500, raise_exception: bool = True):
    """
    把 data 发送给 manager
    socket的 backlog 为1024，超过1024的并发请求可能会变得很慢，请注意

    Args:
         data (dict):
         timeout (int): 设置请求超时时间，默认为 500 秒
         raise_exception (bool): 调用runtime接口时发生异常是否需要抛出，默认为不抛出

    Returns:
         bool: 表示是否通信成功
    """
    b_data = pickle.dumps(data)
    header = str(len(b_data) + 8).rjust(8).encode()
    start_time = time.time()
    result = None
    while True:
        if time.time() - start_time > timeout:
            if raise_exception:
                raise Exception(f'{data.get("source", "")}超时，请检查程序或联系管理员')
            else:
                print(f'{data.get("source", "")}超时，请检查程序或联系管理员')
            return False
        try:
            waiting_time = max(1, timeout - int(time.time() - start_time))
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((f'{user_name()}-{task_id()}-manager-0', 7000))
            s.settimeout(waiting_time)
            s.send(header + b_data)
            result = s.recv(1024)
        except:
            time.sleep(1)
        if result is not None:
            try:
                result = pickle.loads(result)
                success = result['success']
                msg = result['msg']
            except Exception as e:
                if raise_exception:
                    raise Exception(f'解析返回值失败: {e}，请联系管理员')
                else:
                    print(f'解析返回值失败: {e}，请联系管理员')
                    return False
            if success == 0:
                if raise_exception:
                    raise Exception(msg)
                else:
                    print(msg)
                return False
            return True


def set_watchdog_time(seconds: int):
    """
    设置任务超时时间，规定时间内无 log 该任务会被认为已失败，默认为 1800 秒

    Args:
        seconds (int): 超时时间，单位为秒

    Examples:

        >>> from hfai.client import set_watchdog_time
        >>> set_watchdog_time(1800)

    """
    if IS_SIMULATE:
        print(f'模拟设置 watchdog time {seconds} 成功')
        return
    if nb_name() == 'NO_CLUSTER':
        print('非集群环境')
        return
    assert isinstance(seconds, int), "传入的seconds应该是一个int型的"
    assert len(str(seconds)) < 100, "传入的seconds位数应当小于100位"
    WATCHDOG_TIME_SHM_ID_SHM.write(str(seconds).ljust(100))
    return {
        'success': 1,
        'msg': f'set_watchdog_time {seconds}秒 设置成功'
    }


def get_whole_life_state() -> Optional[int]:
    """
    获取当前 chain_id 的上一个 id 任务留下来的 whole_life_state

    Returns:
         int: whole_life_state

    Examples:

        >>> from hfai.client import get_whole_life_state
        >>> get_whole_life_state()

    """
    if IS_SIMULATE:
        return int(os.environ.get('MARSV2_WHOLE_LIFE_STATE', 0))
    if nb_name() == 'NO_CLUSTER':
        print('非集群环境')
        return None
    return int(os.environ.get('MARSV2_WHOLE_LIFE_STATE', 0))


def set_whole_life_state(state: int, timeout: int = 500, raise_exception: bool = True) -> bool:
    """
    设置 whole_life_state

    Args:
         state (int): 想要设置的 whole_life_state
         timeout (int): 设置请求超时时间，默认为 500 秒
         raise_exception (bool): 调用runtime接口时发生异常是否需要抛出，默认为抛出

    Examples:

        >>> from hfai.client import set_whole_life_state
        >>> set_whole_life_state(100)

    """
    if IS_SIMULATE:
        print(f'模拟设置 whole_life_state 成功，请下次调用的时候使用 --sls={state} 来设置生效')
        return True
    if nb_name() == 'NO_CLUSTER':
        print('非集群环境')
        return False
    if os.environ['MARSV2_WHOLE_LIFE_STATE'] == str(state):
        return False
    data = {
        'source': set_whole_life_state.__name__,
        'whole_life_state': state
    }
    return send_data(data, timeout, raise_exception)


# ============== 任务优雅挂起 ===================================================
try:
    import sysv_ipc  # 注意，不兼容 windows
    has_sys_ipc = True
except:
    has_sys_ipc = False

SUSPEND_SHM_ID = 7123378543
if (nb_name() != 'NO_CLUSTER' or IS_SIMULATE) and has_sys_ipc:
    SUSPEND_SHM_ID_SHM = sysv_ipc.SharedMemory(SUSPEND_SHM_ID,
                                               sysv_ipc.IPC_CREAT, mode=0o777,
                                               size=1)
else:
    SUSPEND_SHM_ID_SHM = None


def receive_suspend_command(timeout: int = 500, raise_exception: bool = False) -> bool:
    """
    获取该任务是否即将被打断

    Args:
         timeout (int): 设置请求超时时间，默认为 500 秒
         raise_exception (bool): 调用runtime接口时发生异常是否需要抛出，默认为不抛出

    Returns:
         bool: 表示是否即将被打断

    Examples:

        >>> from hfai.client import receive_suspend_command
        >>> receive_suspend_command()

    """
    if IS_SIMULATE:
        if 0 < SIMULATE_SUSPEND_SEC < (time.time() - IMPORT_TIME):
            print('时间到了，触发模拟打断')
            return True
        return False

    if SUSPEND_SHM_ID_SHM is None:
        return False

    if SUSPEND_SHM_ID_SHM.read() == b'1':
        # 向 server 端报告需要知道我要被打断了
        if nb_name() == 'NO_CLUSTER':
            print('非集群环境')
            return False
        data = {
            'source': receive_suspend_command.__name__
        }
        send_data(data, timeout=timeout, raise_exception=raise_exception)
        return True
    return False


def go_suspend(timeout: int = 500, raise_exception: bool = False):
    """
    通知 server 该任务可以被打断

    Args:
         timeout (int): 设置请求超时时间，默认为 500 秒
         raise_exception (bool): 调用runtime接口时发生异常是否需要抛出，默认为不抛出

    Examples:

        >>> from hfai.client import go_suspend
        >>> go_suspend()

    """
    # 防止 jupyter 容器误跑 go_suspend
    if os.environ.get('MARSV2_TASK_TYPE', '') != 'training':
        return
    # 向 server 端报告需要知道我要被打断了
    if IS_SIMULATE:
        print('模拟打断成功，将退出进程')
        sys.exit(0)

    if nb_name() == 'NO_CLUSTER':
        print('非集群环境')
        return

    data = {
        'source': go_suspend.__name__
    }
    send_data(data, timeout=timeout, raise_exception=raise_exception)

    for i in itertools.count(start=1):
        time.sleep(10)
        print(f'等了{i * 10}秒还没挂起，继续等待')


def set_priority(priority: int, timeout: int = 500, raise_exception: bool = False) -> bool:
    """
    设置当前任务的优先级，注意如果你没有该优先级的权限可能会导致任务被立刻打断

    Args:
         priority (int): 设置的任务优先级
         timeout (int): 设置请求超时时间，默认为 500 秒
         raise_exception (bool): 调用runtime接口时发生异常是否需要抛出，默认为不抛出

    Returns:
        bool: 是否设置成功

    Examples:

        >>> from hfai.client import set_priority, EXP_PRIORITY
        >>> set_priority(EXP_PRIORITY.LOW)

    """
    # 向 server 端报告需要知道我要被打断了
    if IS_SIMULATE:
        print(f'模拟环境设置优先级 {priority} 成功')
        return True
    if nb_name() == 'NO_CLUSTER':
        print('非集群环境')
        return False
    data = {
        'source': set_priority.__name__,
        'priority': priority
    }
    return send_data(data, timeout=timeout, raise_exception=raise_exception)


def disable_warn(warn_type: int, timeout: int = 500, raise_exception: bool = False) -> bool:
    """
    静默warning报警

    Args:
        warn_type (int): 静默的报警类型，可以是WARN_TYPE的复合，0表示不静默任何报警
        timeout (int): 设置请求超时时间，默认为 500 秒
        raise_exception (bool): 调用runtime接口时发生异常是否需要抛出，默认为不抛出

    Returns:
        bool: 是否设置成功

    Examples:

        >>> from hfai.client import disable_warn, WARN_TYPE
        >>> disable_warn(WARN_TYPE.LOG | WARN_TYPE.COMPLETED)  # 日志超时以及completed超时不报警

    """
    if IS_SIMULATE:
        print(f'模拟静默warning报警成功')
        return True
    if nb_name() == 'NO_CLUSTER':
        print('非集群环境')
        return False
    data = {
        'source': disable_warn.__name__,
        'warn_type': warn_type
    }
    return send_data(data, timeout=timeout, raise_exception=raise_exception)


def print_gpu_info(pid):
    if no_pynvml or (not node_name().endswith('dl')):
        return False
    pynvml.nvmlInit()
    for i in range(pynvml.nvmlDeviceGetCount()):
        handle = pynvml.nvmlDeviceGetHandleByIndex(i)
        info = pynvml.nvmlDeviceGetMemoryInfo(handle)
        print(f'[{pid}] gpu[{i}] memory total {info.total}, free {info.free}, used {info.used}; '
              f'Power {pynvml.nvmlDeviceGetPowerState(handle)}')
