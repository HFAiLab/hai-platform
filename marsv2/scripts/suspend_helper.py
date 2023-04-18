import zmq
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5778")
socket.recv()  # 收到打断指令 set_suspend_flag
import sysv_ipc
SUSPEND_SHM_ID_SHM = sysv_ipc.SharedMemory(7123378543, sysv_ipc.IPC_CREAT, mode=0o777, size=1).write(b'1')
socket.send_string('received_0')
socket.recv()  # destroy_suspend_flag 在第二次收到打断指令的时候，把 flag 给销毁掉
SUSPEND_SHM_ID_SHM = sysv_ipc.SharedMemory(7123378543, sysv_ipc.IPC_CREAT, mode=0o777, size=1).write(b'0')
socket.send_string('received_1')
