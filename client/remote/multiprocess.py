import multiprocessing
import os.path
import pickle as pkl
import queue
import shlex
import subprocess
import sys
import threading
import time
from multiprocessing import Process
from multiprocessing import cpu_count
from typing import List, Tuple, Dict


def run_command(command):
    process = subprocess.Popen(shlex.split(command),
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    while True:
        out = process.stdout.readline()
        if not out and process.poll() is not None:  # out is '' and
            break
        if out:
            print(out.decode('utf-8').strip())
    rc = process.poll()
    return rc


class AsyncResult:
    """
    这是由 `Session.apply_async` 和 `Session.map_async` 返回的结果

    实现了和 multiprocessing.pool.AsyncResult 类似的 get() 接口

    Attributes:

        job: 任务名，远程运行时候即提交 hfai 的任务名字
        output: 函数的输出
        output_pkl: outline 模式下为 output 的 pkl 文件位置，inline 为 None
        stdout_pkl: outline 模式下为 stdout 的 pkl 文件位置，inline 为 None

    """
    def __init__(self, job, output_pkl=None, process=None, output_queue=None):
        """
        inline 的构造：job, queue=queue
        outline 的构造：job，output=output, process=process

        Args:
            job: 任务名，远程运行时候即提交 hfai 的任务名字
            output_pkl: outline 模式下为 output 的 pkl 文件位置
            process: 运行任务的后台进程
            output_queue: inline 模式下的输出 queue
        """
        self.job = job
        self.process = process
        self.queue = output_queue
        self.output_pkl = output_pkl
        self.stdout_pkl = (output_pkl + '.stdout.pkl') if output_pkl is not None else None

        # attributes
        self.output = None
        del self.output # make self.output undefined
        self.stdout_lines = []

        assert not (self.process is not None and self.queue is not None), '注意，inline 和 outline 的构造不一样'

    def get(self, timeout=None, stdout=True):
        """
        用于异步运行时候获取函数输出结果，超时则会关闭运行进程

        :param: timeout, 若为 None，则等待一年, 如果 timeout 之后，没有结果，杀掉进程，raise multiprocessing.TimeoutError
        :param: 是否要在 ar get 的时候输出运行的 stdout，默认输出
        :return: 函数运行结果
        """
        if hasattr(self, 'output'):
            if isinstance(self.output, Tuple) and len(self.output) == 3 \
                    and self.output[0] == 'remote_call_exception':
                print(self.output[2])
                raise self.output[1]
            return self.output
        # inline
        if self.queue:
            try:
                self.output = self.queue.get(timeout=timeout)
                return self.get()
            except queue.Empty:
                self.process.kill()
                raise multiprocessing.TimeoutError()
        # outline
        process = self.process
        if process is not None:
            timer = threading.Timer(timeout if timeout else 60 * 60 * 24 * 365, process.kill)
            try:
                timer.start()
                while True:
                    out = process.stdout.readline()
                    if not out and process.poll() is not None:  # out is '' and
                        break
                    if out:
                        if stdout:
                            sys.stdout.write(out.decode('utf-8'))
                        self.stdout_lines.append(out)
                process.poll()
            finally:
                timer.cancel()
        pkl.dump(self.stdout_lines, open(self.stdout_pkl, 'wb+'))

        try:
            self.output = pkl.load(open(self.output_pkl, 'rb'))
            return self.output
        except pkl.PickleError:
            # pkl 失败我就任务进程失败
            raise multiprocessing.TimeoutError

    def stdout(self):
        """
        直接 stdout 输出日志, 对于 outline 的运行，情况，如果 get 和运行 cell 不在一起会丢失输出
        inline 的情况下，就和 process Pool 表现一致，不额外处理了

        :return:
        """
        if self.stdout_pkl and os.path.exists(self.stdout_pkl):
            stdout_lines = pkl.load(open(self.stdout_pkl, 'rb'))
            for out in stdout_lines:
                sys.stdout.write(out.decode('utf-8'))

    def job_log(self) -> None:
        """
        若在集群上运行，可以通过这个接口来查看日志，直接 stdout 输出日志

        :return None
        """
        hfai_cmd = f'hfai logs {self.job}'
        run_command(hfai_cmd)


class Pool:
    def __init__(self, process_limit=0):
        self.process_limit = process_limit or cpu_count()
        self.process_queue = queue.Queue()  # queue for thread
        self.running_processes: List[Process] = []

        t = threading.Thread(target=self.start_process_thread)
        t.setDaemon(True)
        t.start()

    def set_process_limit(self, process_limit=0):
        self.process_limit = process_limit or cpu_count()

    def apply_async(self, name, target, args: Tuple = (), kwargs: Dict = {}):
        def wrap(_func, _queue):
            def wrapper(*_args, **_kwargs):
                try:
                    # 如果这里 crash 了，那么会 blocking 住 queue
                    out = _func(*_args, **_kwargs)
                except Exception as e:
                    out = e
                _queue.put(out)

            return wrapper

        q = multiprocessing.Queue()
        p = Process(name=name, target=wrap(target, q), args=args, kwargs=kwargs)
        p.daemon = True
        self.process_queue.put(p)
        async_result = AsyncResult(job=name, output_queue=q)
        return async_result

    def start_process_thread(self):
        while True:
            self.running_processes = [rp for rp in self.running_processes if rp.is_alive()]
            if len(self.running_processes) < self.process_limit:
                p: Process = self.process_queue.get()
                p.start()
                self.running_processes.append(p)
            else:
                # small delay for reduce cpu load
                time.sleep(0.01)
