from api.task_schema import TaskSchema
from base_model.base_task import ITaskImpl


def parse_code_cmd(task_impl: ITaskImpl):
    """
    # 可以自定义在用户的代码中注入一些东西

    :param task_impl:
    :return: code_dir, code_file, code_params
    """
    task_schema: TaskSchema = TaskSchema.parse_obj(task_impl.task.schema)
    return task_schema.spec.workspace, task_schema.spec.entrypoint, task_schema.spec.parameters
