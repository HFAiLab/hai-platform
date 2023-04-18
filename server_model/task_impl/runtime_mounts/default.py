from base_model.base_task import ITaskImpl


def add_runtime_mounts(task_impl: ITaskImpl):
    """
    在 task_impl 中添加动态的挂载点，这样可以在 schema 的时候调用进去
    # 一般而言，挂载点是在 storage 表中的，但是会有在运行环境中指定的额外挂载点

    举例：
    task.runtime_mounts.append({
        'host_path': mount_src_path,
        'mount_path': mountpath,
        'mount_type': 'DirectoryOrCreate',
        'read_only': False,
        'name': 'workspace-path'
    })

    :param task_impl:
    :return:
    """
    pass
