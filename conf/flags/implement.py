
from .default import *
from .custom import *

try:
    from ..server_flags import *
except Exception as e:
    print(e)
    pass
import collections
import os


class EXP_STATUS:
    QUEUED = 'queued'
    CREATED = 'created'
    BUILDING = 'building'
    RUNNING = 'running'
    UNSCHEDULABLE = 'unschedulable'
    TERMINATING = 'terminating'

    SUCCEEDED_TERMINATING = 'succeeded_terminating'
    FAILED_TERMINATING = 'failed_terminating'
    STOPPED_TERMINATING = 'stopped_terminating'

    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
    STOPPED = 'stopped'

    MIXED = 'mixed'
    UNFINISHED = [CREATED, RUNNING, BUILDING, UNSCHEDULABLE,
                  SUCCEEDED_TERMINATING, FAILED_TERMINATING, STOPPED_TERMINATING]
    # 结束态
    ENDING = [STOPPED, FAILED, SUCCEEDED, SUCCEEDED_TERMINATING, FAILED_TERMINATING, STOPPED_TERMINATING]
    FINISHED = [STOPPED, FAILED, SUCCEEDED]
    UPGRADE_FINISHED = [FAILED, SUCCEEDED]


class QUE_STATUS:
    QUEUED = 'queued'  # 任务在队列中
    SCHEDULED = 'scheduled'  # 任务被调度到了
    FINISHED = 'finished'  # 任务结束


STATUS_COLOR_MAP = {
    EXP_STATUS.QUEUED: 'blue',
    EXP_STATUS.RUNNING: 'yellow',
    EXP_STATUS.STOPPED: 'gray',
    EXP_STATUS.SUCCEEDED: 'green',
    EXP_STATUS.FAILED: 'red',

    QUE_STATUS.QUEUED: 'blue',
    QUE_STATUS.SCHEDULED: 'yellow',
    QUE_STATUS.FINISHED: 'red',
}


class STOP_CODE:
    NO_STOP          = 0b00000000000000  # 0
    STOP             = 0b00000000000001  # 1  # SUCCESS # 因为我们首先接受到的就是 stop 指令
  # INIT_FAILED      = 0b00000000000010  # 2
  # FAILED           = 0b00000000000100  # 4
  # TIMEOUT          = 0b00000000001000  # 8
    INTERRUPT        = 0b00000000010000  # 16
    UNSCHEDULABLE    = 0b00000000100000  # 32
  # MANUAL_STOP      = 0b00000001000000  # 64
    FAILED           = 0b00000010000000  # 128
    INIT_FAILED      = 0b00000100000000  # 256
    TIMEOUT          = 0b00001000000000  # 512
    MANUAL_STOP      = 0b00010000000000  # 1024
    HOOK_RESTART     = 0b00100000000000  # 2048
    MANUAL_SUCCEEDED = 0b01000000000000  # 4096
    MANUAL_FAILED    = 0b10000000000000  # 8192

    def __init__(self):
        self.action_map = collections.OrderedDict()
        for k in self.__class__.__dict__:
            if isinstance(self.__class__.__dict__[k], int):
                self.action_map[self.__class__.__dict__[k]] = k

    def name(self, action):
        for k in reversed(self.action_map.keys()):
            if action >= k:
                return self.action_map[k]
        return 'NAN'


class SUSPEND_CODE:
    NO_SUSPEND = 0  # 不需要打断
    SUSPEND_SENT = 1  # 发送打断通知
    SUSPEND_RECEIVED = 2  # 任务节点表示收到打断通知
    CAN_SUSPEND = 3  # 任务表示可以被打断我了


class TASK_TYPE:
    UPGRADE_TASK = 'upgrade'
    TRAINING_TASK = 'training'
    JUPYTER_TASK = 'jupyter'
    VIRTUAL_TASK = 'virtual'
    VALIDATION_TASK = 'validation'
    BACKGROUND_TASK = 'background'

    @classmethod
    def all_task_types(cls):
        return [getattr(TASK_TYPE, t) for t in cls.__dict__.keys() if t.isupper()]


class CHAIN_STATUS:
    WAITING_INIT = 'waiting_init'
    RUNNING = 'running'
    SUSPENDED = 'suspended'
    FINISHED = 'finished'


def chain_status_to_queue_status(cs):
    if cs in [CHAIN_STATUS.WAITING_INIT, CHAIN_STATUS.SUSPENDED]:
        return QUE_STATUS.QUEUED
    if cs == CHAIN_STATUS.RUNNING:
        return QUE_STATUS.SCHEDULED
    if cs == CHAIN_STATUS.FINISHED:
        return QUE_STATUS.FINISHED


class TASK_FLAG:
    SUSPEND_CODE = 0b00000011  # 最后两位表示SUSPEND_CODE
    STAR =         0b00000100  # 表示是否被用户收藏


class EXP_PRIORITY:
    if os.environ.get('external') != 'true':
        EXTREME_HIGH = 50
        VERY_HIGH = 40
        HIGH = 30
        ABOVE_NORMAL = 20
    AUTO = -1

    @classmethod
    def get_name_by_value(cls, value):
        for k, v in cls.__dict__.items():
            if v == value:
                return k
        return 'AUTO'


class WARN_TYPE:
    COMPLETED =   0b0001  # 卡在completed 30分钟
    TERMINATING = 0b0010  # 卡在terminating 30分钟
    LOG =         0b0100  # 日志超时
    INTERRUPT =   0b1000  # 被打断


class PARLIAMENT_MEMBERS:
    SENATOR = 'senator'
    MASS = 'mass'


class PARLIAMENT_SOURCE_TYPE:
    CREATE_ARCHIVE = 'create_archive'
    CANCEL_MASS = 'cancel_mass'
    REGISTER_MASS = 'register_mass'
    UPDATE = 'update'
    CONNECT = 'connect'
    CANCEL_ARCHIVE = 'cancel_archive'
