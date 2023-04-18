

import os
from roman_parliament.archive import register_archive, archive_dict
from roman_parliament.utils import generate_key
from server_model.selector import TrainingTaskSelector
from base_model.training_task import TrainingTask
from server_model.auto_task_impl import AutoTaskSchemaImpl
from conf.flags import TASK_TYPE
from .base_trigger import BaseTrigger


LAUNCHER_COUNT = int(os.environ['LAUNCHER_COUNT'])
CURRENT_LAUNCHER = int(os.environ['REPLICA_RANK']) if os.environ.get('MODULE_NAME', '') == 'launcher' and 'REPLICA_RANK' in os.environ else -1


class LauncherTaskTrigger(BaseTrigger):
    """
    launcher 专用，订阅启动任务的 trigger
    """
    @classmethod
    def create_archive(cls, data):
        if CURRENT_LAUNCHER >= 0:
            for task_id, assigned_launcher_id in data.items():
                if assigned_launcher_id != CURRENT_LAUNCHER:
                    continue
                archive_key = generate_key(f'registered_{TrainingTask.__name__}', 'id', task_id)
                if archive_key not in archive_dict:
                    task = TrainingTaskSelector.find_one_by_id(AutoTaskSchemaImpl, id=task_id)
                    if task.task_type in [TASK_TYPE.JUPYTER_TASK, TASK_TYPE.TRAINING_TASK, TASK_TYPE.VALIDATION_TASK, TASK_TYPE.BACKGROUND_TASK]:
                        register_archive(archive=task, sign='id')
