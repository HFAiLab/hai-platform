

import pickle
import pandas as pd


class TickData(object):
    def __init__(
            self,
            seq=0,
            valid=False,
            resource_df: pd.DataFrame = pd.DataFrame(columns=[
                'cpu', 'nodes', 'name', 'gpu_num', 'status', 'mars_group', 'memory', 'group', 'working', 'leaf',
                'spine', 'active', 'schedule_zone', 'working_user_role', 'origin_group', 'allocated'
            ]),
            user_df: pd.DataFrame = pd.DataFrame(columns=[
                'user_name', 'resource', 'group', 'quota', 'role', 'priority', 'active'
            ]),
            task_df: pd.DataFrame = pd.DataFrame(columns=[
                'id', 'nb_name', 'user_name', 'code_file', 'group', 'nodes', 'assigned_nodes', 'backend', 'task_type',
                'queue_status', 'priority', 'first_id', 'running_seconds', 'chain_id', 'config_json', 'user_role',
                'assign_result', 'match_result', 'scheduler_msg', 'created_seconds', 'custom_rank',
                'worker_status', 'memory', 'cpu', 'assigned_gpus', 'schedule_zone', 'client_group', 'current_schedule_zone',
                'is_spot_jupyter', 'match_rank', 'runtime_config_json'
            ]),
            extra_data: dict = None,
            metrics: dict = None
    ):
        # 当前 seq
        self.seq = seq
        # 这个数据是不是有效的
        self.valid = valid
        # df 信息
        self.resource_df = resource_df
        self.user_df = user_df
        self.task_df = task_df
        # 一些组件的 tick_data 不依赖 df，是自己记录的东西，写在这里
        self.extra_data = extra_data if extra_data is not None else {}
        self.metrics = metrics if metrics is not None else {}

    @classmethod
    def dumps(cls, instance: "TickData") -> bytes:
        return pickle.dumps(instance)

    @classmethod
    def loads(cls, dumped_instance: bytes) -> "TickData":
        return pickle.loads(dumped_instance)


class ASSIGN_RESULT:
    CAN_RUN = 'CAN_RUN'
    CAN_NOT_RUN = 'CAN_NOT_RUN'
    NOT_SURE = 'NOT_SURE'
    OUT_OF_QUOTA = 'OUT_OF_QUOTA'
    NO_SUCH_GROUP = 'NO_SUCH_GROUP'
    NODE_ERROR = 'NODE_ERROR'
    RE_MATCH = 'RE_MATCH'
    MATCH_ERROR = 'MATCH_ERROR'
    RESOURCE_NOT_ENOUGH = 'RESOURCE_NOT_ENOUGH'
    QUOTA_EXCEEDED = 'QUOTA_EXCEEDED'


class MATCH_RESULT:
    NOT_SURE = 'NOT_SURE'
    KEEP_RUNNING = 'KEEP_RUNNING'
    DO_NOTHING = 'DO_NOTHING'
    SUSPEND = 'SUSPEND'
    STARTUP = 'STARTUP'
    STOP = 'STOP'
