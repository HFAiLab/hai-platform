import pickle
import threading
import socket
import os
from experiment_manager.manager.client_handler import set_whole_life_state, \
    receive_suspend_command, go_suspend, set_priority, disable_warn, waiting_memory_free_failed, report_git_revision
from experiment_manager.manager.manager_utils import get_log_uuid
from logm import logger, log_stage

READ_BUF = 1024
module = os.path.basename(__file__)
log_id = get_log_uuid(module)

dealer_func_dict = {
    set_whole_life_state.__name__: set_whole_life_state,
    receive_suspend_command.__name__: receive_suspend_command,
    go_suspend.__name__: go_suspend,
    set_priority.__name__: set_priority,
    disable_warn.__name__: disable_warn,
    waiting_memory_free_failed.__name__: waiting_memory_free_failed,
    report_git_revision.__name__: report_git_revision,
}


class Reader(threading.Thread):
    def __init__(self, client):
        threading.Thread.__init__(self)
        self.client = client

    @log_stage(log_id)
    def run(self):
        data_list = []
        header = None
        length = 0
        while True:
            data = self.client.recv(READ_BUF)
            if header is None:
                header = data[:8]
            length += len(data)
            data_list.append(data)
            if length == int(header):  # 说明已经结束了
                break
        b_data = b''.join(data_list)[8:]
        try:
            data = pickle.loads(b_data)
            logger.info(f'收到 {data}')
            try:
                result = dealer_func_dict[data['source']](**data)
            except Exception as e:
                logger.error(f'用户调用 runtime 接口异常，data: {data}; exception: {e}')
                result = {
                    'success': 0,
                    'msg': '用户调用 runtime 接口异常，请联系管理员'
                }
        except Exception as e:  # 传过来的消息不对
            logger.error(f'解析出问题了，raw data: {b_data}; exception: {e}')
            result = {
                'success': 0,
                'msg': '数据解析出问题了，请联系管理员'
            }
        self.client.send(pickle.dumps(result))
        self.client.close()


class Listener(threading.Thread):
    @log_stage(log_id)
    def __init__(self, port):
        threading.Thread.__init__(self)
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", port))
        self.sock.listen(1024)
        logger.info(f'启动成功，正开始监听: {port}')

    def run(self):
        while True:
            client, _ = self.sock.accept()
            Reader(client).start()


Listener(7000).start()
