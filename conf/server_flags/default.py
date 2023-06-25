
# 这里是只有 server 端能看到的 conf
import os
from enum import Enum


BACKEND_UPGRADE = 'upgrade'
UPGRADE_FAILED_VERSION = '0'
ROOT_GROUP_NAME = 'marsV2'
VALIDATION_TASK_FLAG = 'T6DVhD'

queue_job_channel = f'PLX_QUEUE_JOB_CHANNEL'
queue_job_list = f'PLX_QUEUE_JOB_LIST'
run_queue_key = f'PLX_RUN_QUEUE_KEY'
run_job_key = f'PLX_RUN_JOB_KEY'


class CreateCode(Enum):
    DB_SUCCESS = 0
    SUCCESS = 1
    NO_RESOURCE = 2
    JOB_FAIL = 3
    DIST_ERROR = 4
    DB_DOWN = 5
    SCHEMA_ERROR = 6
    PLX_FAIL = 7
    START_SUCCESS = 8
    STOP_SUCCESS = 9


class ConsumerMethod(Enum):
    RUN_QUEUE = 1
    RUN_JOB = 2


class RunJobCode(Enum):
    FATAL = 0
    DONE = 0
    SUCCESS = 1
    QUEUED = 1
    EXISTS = 0


# 分时任务可以打断的运行时长阈值
TIME_SHARING_TASK_THRESHOLD_MIN = int(os.environ.get('TIME_SHARING_TASK_THRESHOLD_MIN', 240))

# 调度lock节点的时间
REDIS_LOCK_SECONDS = int(os.environ.get('REDIS_LOCK_SECONDS', 30))


MOUNT_CODE = {
}


# 任务的优先级对应关系，这里step为10，防止之后有新增优先级
# 注意这里的顺序必须严格递减
class TASK_PRIORITY(int, Enum):
    EXTREME_HIGH = 50
    VERY_HIGH = 40
    HIGH = 30
    ABOVE_NORMAL = 20
    NORMAL = 10
    UNDER_NORMAL = 8
    BELOW_NORMAL = 5
    LOW = 0
    AUTO = -1

    @classmethod
    def items(cls):
        return [(item.name, item.value) for item in cls]

    @classmethod
    def keys(cls):
        return [item.name for item in cls]

    @classmethod
    def values(cls):
        return [item.value for item in cls]

    @classmethod
    def value_key_map(cls):
        return {i.value: i.name for i in cls}

    @classmethod
    def max_external_priority(cls):
        return cls.BELOW_NORMAL

    @classmethod
    def min_internal_priority(cls):
        return cls.UNDER_NORMAL

    @classmethod
    def external_priorities(cls, with_auto=False):
        if with_auto:
            return [p for p in cls if p <= cls.max_external_priority() or p == cls.AUTO]
        else:
            return [p for p in cls if p <= cls.max_external_priority() and p != cls.AUTO]

    @classmethod
    def internal_priorities(cls, with_auto=False):
        if with_auto:
            return [p for p in cls if p >= cls.min_internal_priority() or p == cls.AUTO]
        else:
            return [p for p in cls if p >= cls.min_internal_priority() and p != cls.AUTO]

    @classmethod
    def all_priorities(cls, with_auto=False):
        if with_auto:
            return [p for p in cls]
        else:
            return [p for p in cls if p != cls.AUTO]


# 调度的分界时间（分钟数）
SCHEDULE_RUNNING_MIN_TIME = 15


# 调度算法分界阈值（秒数）
SCHEDULE_THRESHOLD_SECOND = 900


# 调度半衰期（秒）
SCHEDULE_HALF_LIFE = 3600


class TASK_OP_CODE(str, Enum):
    STOP = 'stop'
    SUSPEND = 'suspend'
    SUCCEED = 'succeed'
    FAIL = 'fail'


NODE_FLAG = {}
