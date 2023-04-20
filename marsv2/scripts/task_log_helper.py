"""
注:
    脚本会使用任务镜像中的 python 环境运行, 需要兼容至最低 pyton3.6,
    且引入包依赖时需要同步修改 /marsv2/scritps/validate_image.sh
"""

import glob
import functools
import json
import os
import pickle
import shlex
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from typing import Dict

import psutil
import sysv_ipc
import zmq

WATCHDOG_TIME_SHM_ID = 237965198
try:
    WATCHDOG_TIME_SHM_ID_SHM = sysv_ipc.SharedMemory(WATCHDOG_TIME_SHM_ID, sysv_ipc.IPC_CREX, mode=0o777)
except sysv_ipc.ExistentialError:
    WATCHDOG_TIME_SHM_ID_SHM = sysv_ipc.SharedMemory(WATCHDOG_TIME_SHM_ID)

MAX_LOG_SIZE_PER_SERVICE = 10 * 1024 ** 2


context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.bind("tcp://*:5775")
current_time = time.time()
time.sleep(0.1)


print_lock = Lock()
def lock_print(*args, print_func, **kwargs):
    with print_lock:    # 防止日志串行
        print_func(*args, **kwargs, flush=True)
print = functools.partial(lock_print, print_func=print)


socket_lock = Lock()
def send_string(*args, **kwargs):
    with socket_lock:
        socket.send_string(*args, **kwargs)


def update_time():
    global current_time
    inp = sys.stdin.read(1)
    while inp:
        inp = sys.stdin.read(1)
        current_time = time.time()


def check_timeout_jupyter():
    if os.environ['MARSV2_USER_ROLE'] == 'internal':
        # 内部用户的 service task 不超时
        return
    is_shared_group = os.environ.get('MARSV2_SHARED_JUPYTER', '1') == '1'

    start_time = time.time()
    default_watchdog_time = (3600 * 3) if is_shared_group else (3600 * 12)  # 独占默认 12 个小时, 共享默认 3 个小时
    watchdog_time = default_watchdog_time
    while True:
        watchdog_time_in_shm = WATCHDOG_TIME_SHM_ID_SHM.read().decode().strip()
        watchdog_time_in_shm = int(watchdog_time_in_shm) if watchdog_time_in_shm else default_watchdog_time
        if watchdog_time_in_shm < watchdog_time:
            # 屏蔽 watchdog time 的减少, 防止 Jupyter 中调试代码时误调用 set_watchdog_time 导致 Jupyter 超时
            WATCHDOG_TIME_SHM_ID_SHM.write(str(watchdog_time).ljust(100))
        else:
            watchdog_time = watchdog_time_in_shm
        # 实际上再给 5 分钟
        time_delta = time.time() - start_time - 300
        if time_delta >= watchdog_time:
            send_string(json.dumps({
                'event_type': 'timeout',
                'msg': f'自定义 jupyter 超时{int(watchdog_time / 60)}分钟，目前已经运行 {int(time_delta / 60)} 分钟，即将关闭',
            }, ensure_ascii=False))
            break
        time.sleep(60)


def check_timeout_training():
    global current_time
    try:
        DEFAULT_WATCHDOG_TIME = int(os.environ['HF_WATCHDOG_TIME'])
    except Exception:
        DEFAULT_WATCHDOG_TIME = 1800  # 默认为30分钟
    sleep_time = 1 if 'test_3_test_timeout' in os.environ.get('MARSV2_NB_NAME', '') else 60

    while True:
        watchdog_time = WATCHDOG_TIME_SHM_ID_SHM.read().decode().strip()
        time_delta = time.time() - current_time
        watchdog_time = int(watchdog_time) if watchdog_time else DEFAULT_WATCHDOG_TIME
        if time_delta >= watchdog_time:
            send_string(json.dumps({
                'event_type': 'timeout',
                'msg': f'自定义超时{int(watchdog_time / 60)}分钟，目前已有{int(time_delta / 60)}分钟没有日志，疑似卡住，将所有节点标记成failed，即将关闭',
            }, ensure_ascii=False))
        time.sleep(sleep_time)


def safe_getpgid(pid: int):
    try:
        return os.getpgid(pid)
    except ProcessLookupError:
        return None


def pgid_exists(pgid: int):
    return pgid in map(safe_getpgid, psutil.pids())


class ServiceManager:
    def __init__(self, service_name, config: Dict):
        self.service_name = service_name
        self.config = config
        self.alive = False
        self.pid = None
        self.pgid = None
        self.exit_code = 0
        self.restart_cnt = 0
        self.watch_thread = threading.Thread(target=self.launch_and_watch)
        self.watch_thread.start()

    def print(self, *args, **kwargs):
        print(f'[{self.service_name}]', *args, **kwargs)

    def get_pid(self, runuser_pid):
        for _ in range(5):
            try:
                runner_proc = psutil.Process(runuser_pid)
            except psutil.NoSuchProcess:
                return 'Unknown(1)'     # runuser 进程很快结束了, 拿不到也不需要 pid
            if runner_proc.status() == 'zombie':
                return 'Unknown(2)'     # runuser 进程 zombie, 即子进程已经结束了, 拿不到也不需要 pid
            children = runner_proc.children()
            for child in children:
                if safe_getpgid(child.pid) == child.pid:    # find process group leader
                    return child.pid
            time.sleep(1)           # runuser 进程存在且不是 zombie 状态的情况下没有子进程, 认为是 runuser 还没来得及启动
        return 'Unknown(3)'

    def signal_state(self, alive, msg):
        self.print(msg)     # 状态变化同时展示在容器日志里
        send_string(json.dumps({
            'event_type': 'service_status_change',
            'service_name': self.service_name,
            'alive': alive,
            'msg': msg,
        }, ensure_ascii=False))

    def kill_process_group(self):
        try:
            os.killpg(self.pgid, signal.SIGTERM)
            for _ in range(5):  # 等待 5s graceful shutdown
                if not pgid_exists(self.pgid):
                    return
                time.sleep(1)
            os.killpg(self.pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except Exception as e:
            self.print(f'kill 进程组 {self.pgid} 失败: {e}')

    @property
    def log_file_path(self):
        return f'{os.environ.get("MARSV2_LOG_FILE_PATH")}.{self.service_name}.service_log'

    def rotate_service_log(self):
        logs = glob.glob(self.log_file_path + '*')
        sum_log_size = sum(os.path.getsize(f) for f in logs)
        if sum_log_size <= MAX_LOG_SIZE_PER_SERVICE:
            return
        self.print(f'日志文件大小超限 ({sum_log_size / 1024 ** 2:.1f}M > {MAX_LOG_SIZE_PER_SERVICE // 1024 ** 2}M), 清理旧的服务日志')
        for file in sorted(logs, key=lambda f: os.path.getmtime(f)):
            sum_log_size -= os.path.getsize(file)
            os.remove(file)
            if sum_log_size <= MAX_LOG_SIZE_PER_SERVICE:
                return

    def launch_and_watch(self):
        try:
            self.rotate_service_log()
            self.restart_cnt += 1
            flag_line = f'[start service [{self.service_name}] of task ' \
                        f'[{os.environ.get("MARSV2_NB_NAME")}({os.environ.get("MARSV2_TASK_ID")})] ' \
                        f'on {os.environ.get("MARSV2_NODE_NAME")} for {os.environ.get("MARSV2_USER")}]'
            log_pipe_chain = f"ts '[%Y-%m-%d %H:%M:%.S]' | pv -L ${{MAX_OPS}} 2>/dev/null" \
                             f" | rotatelogs -n ${{NUMBER_OF_FILES}}" \
                             f" {self.log_file_path}.{self.restart_cnt} ${{MAX_FILESIZE}}"
            cmd = f"set -o pipefail; " \
                  f"(echo {shlex.quote(flag_line)} && ({self.config.get('startup_script')})) " \
                  f"2>&1 | {log_pipe_chain}"
            cmd = f"MARSV2_SERVICE_NAME={self.service_name} MARSV2_SERVICE_PORT={self.config.get('port')} " \
                  "/sbin/runuser --fast ${MARSV2_USER} --preserve-environment -c " + shlex.quote(cmd)
            proc = subprocess.Popen(cmd, shell=True, executable='/bin/bash')
            self.pid = self.get_pid(runuser_pid=proc.pid)
            self.pgid = os.getpgid(self.pid) if isinstance(self.pid, int) else None
            self.alive = True
            self.signal_state(alive=True, msg=f'服务已启动, PID: {self.pid}, PGID: {self.pgid}')
            proc.wait()
            if self.pgid is not None and pgid_exists(self.pgid):
                self.print(f'父进程已退出, 但进程组仍然存在, 开始 kill 进程组 PGID={self.pgid}.')
                self.kill_process_group()
            self.alive = False
            # runuser 会将子进程 kill signal 加 128 之后作为自己的 return code, 这里还原一下方便看
            self.exit_code = proc.returncode if proc.returncode <= 128 else f'{proc.returncode}(128+{proc.returncode-128})'
            msg = f'服务进程结束, PID: {self.pid}, exit_code={self.exit_code}'
            self.signal_state(alive=False, msg=msg)
        except Exception as e:
            self.print(f'启动服务进程失败: {e}')

    def stop(self):
        self.print(f'尝试结束服务')
        if not self.alive or not self.watch_thread.is_alive():
            self.print('服务进程未在运行, 忽略操作')
            return
        if self.pgid is None:
            self.print('未获取到 PGID, 无法结束进程')      # 正常情况下不会出现
            return
        try:
            # 先尝试 kill -15, 五秒杀不掉就 kill -9
            os.killpg(self.pgid, signal.SIGTERM)
            self.watch_thread.join(timeout=10)
            if self.watch_thread.is_alive():
                # 有 SIGKILL 兜底, 理论上不会出现杀不掉进程导致 join 超时的情况
                self.print(f'等待 10s 后进程或进程组仍然存在, 请手动杀死服务进程(PID: {self.pid}, PGID: {self.pgid})')
        except ProcessLookupError: # killpg 找不到指定进程组, 即进程已退出
            pass
        except Exception as e:
            self.print(f'结束服务失败 ({e})')

    def start(self):
        self.print(f'尝试启动服务')
        if self.alive or self.watch_thread.is_alive():
            self.print('服务进程仍在运行, 忽略操作')
            return
        self.watch_thread = threading.Thread(target=self.launch_and_watch)
        self.watch_thread.start()

    def restart(self):
        if self.alive or self.watch_thread.is_alive():
            self.stop()
        self.start()


def manage_services():
    recv_socket = zmq.Context().socket(zmq.PULL)
    recv_socket.connect(f'tcp://{os.environ["MARSV2_USER"].replace("_", "-")}-{os.environ["MARSV2_TASK_ID"]}-manager-0:5776')
    thread_pool = ThreadPoolExecutor(max_workers=8)

    services = json.loads(os.environ.get('SERVICES', '{}'))
    rank = int(os.environ.get('MARSV2_RANK'))
    if len(services) == 0:
        return
    print('容器已启动, 正在启动服务')
    managers = {}
    for service_name, service in services.items():
        if rank in service['rank'] or -1 in service['rank']:
            managers[service_name] = ServiceManager(service_name, config=service)
    if len(managers) == 0:
        print(f'rank={rank} 无服务需要启动, 跳过')

    while True:
        request = None
        try:
            request = pickle.loads(recv_socket.recv())
            action, service = request.get('action'), request.get('service')
            if service not in managers:
                print(f'请求 {action} 的服务 {service} 不存在, 忽略操作')
                continue
            print(f'用户操作 {action} 服务 {service}')
            manager = managers.get(service)
            if action == 'stop':
                thread_pool.submit(manager.stop)
            elif action == 'start':
                thread_pool.submit(manager.start)
            elif action == 'restart':
                thread_pool.submit(manager.restart)
        except Exception as e:
            print(f'处理请求失败: {e}, 请求: {request}')


task_type = os.environ.get('MARSV2_TASK_TYPE', 'training')
th1 = threading.Thread(target=update_time)
th1.start()
th2 = threading.Thread(target=check_timeout_jupyter if task_type == 'jupyter' else check_timeout_training, daemon=True)
th2.start()
th3 = threading.Thread(target=manage_services, daemon=True)
th3.start()
