

from abc import ABC
from conf.flags import TASK_TYPE
from base_model.base_task import ITaskImpl
from server_model.virtual_task_impl import VirtualTaskApiImpl
from server_model.service_task_impl import ServiceTaskSchemaImpl
from server_model.training_task_impl import TaskApiImpl, AdditionalPropertyImpl
from server_model.task_impl import DbOperationImpl

"""
根据 task_type 自动选择对应的 implement，方便 manager / api server 进行操作
目前看来暂时不需要写 AutoTaskSelector 类，因为 TrainingTaskSelector 能满足所有需求
"""


# 这里可以直接继承 object，为了方便查看有的方法，继承 ITaskImpl
class AutoTaskImpl(ITaskImpl, ABC):

    impl_mapping = {}

    def __new__(cls, *args, **kwargs):
        task = args[0] if len(args) > 0 else kwargs.get('task')
        return cls.impl_mapping.get(task.task_type, cls.impl_mapping[TASK_TYPE.TRAINING_TASK])(task)


class AutoTaskApiImpl(AutoTaskImpl, ABC):

    impl_mapping = {
        TASK_TYPE.TRAINING_TASK: TaskApiImpl,
        TASK_TYPE.VIRTUAL_TASK: VirtualTaskApiImpl
    }


class AutoTaskSchemaImpl(AutoTaskImpl, ABC):

    impl_mapping = {
        TASK_TYPE.TRAINING_TASK: AdditionalPropertyImpl,
        TASK_TYPE.JUPYTER_TASK: ServiceTaskSchemaImpl
    }


class AutoTaskSchemaWithDbImpl(AutoTaskImpl, ABC):

    impl_mapping = {
        TASK_TYPE.TRAINING_TASK: type('_'.join([DbOperationImpl.__name__, AdditionalPropertyImpl.__name__]), (DbOperationImpl, AdditionalPropertyImpl), dict()),
        TASK_TYPE.JUPYTER_TASK: type('_'.join([DbOperationImpl.__name__, ServiceTaskSchemaImpl.__name__]), (DbOperationImpl, ServiceTaskSchemaImpl), dict())
    }
