

from base_model.base_task import ITaskImpl


def get_runtime_sidecars(task_impl: ITaskImpl, rank, schema):
    return schema
