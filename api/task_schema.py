from typing import Dict, Any, List, Optional

from pydantic import BaseModel  # 和 py36 不兼容

from conf.flags import TASK_TYPE


class TaskSpec(BaseModel):
    workspace: str
    entrypoint: str
    parameters: str = ''
    environments: Dict[str, str] = {}
    entrypoint_executable: bool = False


class TaskResource(BaseModel):
    image: str = 'default'
    group: str = 'default'
    node_count: int = 1
    is_spot: bool = False  # 抢占式
    gpu: int = 0      # 非独占，限制 gpu 个数，0 为不限制
    cpu: int = 0      # 非独占，限制 cpu 个数，0 为不限制
    memory: int = 0   # 非独占，限制 内存，0 为不限制


class TaskService(BaseModel):
    name: str
    port: int = None    # 可以为空, 例如 built-in config
    type: str = None
    rank: List[int] = [0]
    startup_script: str = None


class TaskSchema(BaseModel):
    """
    见 client.api.experiment_api 的 create_experiment
    """
    version: int
    name: str
    task_type: str = TASK_TYPE.TRAINING_TASK
    priority: int = -1  # 默认 auto
    spec: Optional[TaskSpec] = None # 仅 jupyter 任务允许为 None
    resource: TaskResource
    options: Dict[str, Any] = {}
    services: List[TaskService] = []
