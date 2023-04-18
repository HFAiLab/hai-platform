import zmq
import sysv_ipc
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5779")
socket.recv()
sysv_ipc.SharedMemory(7060593987, sysv_ipc.IPC_CREAT, mode=0o777, size=1).write(b'1')
