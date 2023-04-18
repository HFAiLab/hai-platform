from roman_parliament.archive import register_archive
from server_model.selector import TrainingTaskSelector
from server_model.auto_task_impl import AutoTaskSchemaImpl
from conf.flags import TASK_TYPE
from .base_trigger import BaseTrigger


class TrainingTaskTrigger(BaseTrigger):
    @classmethod
    def create_archive(cls, data):
        for task_id in data:
            task = TrainingTaskSelector.find_one_by_id(AutoTaskSchemaImpl, id=task_id)
            if task.task_type in [TASK_TYPE.JUPYTER_TASK, TASK_TYPE.TRAINING_TASK, TASK_TYPE.VALIDATION_TASK]:
                register_archive(archive=task, sign='id')
