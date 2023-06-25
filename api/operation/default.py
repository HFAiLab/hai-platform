
from typing import Dict

from api.task_schema import TaskSchema
from base_model.base_task import BaseTask
from server_model.selector import TrainImageSelector, AioTrainEnvironmentSelector


async def check_environment_get_err(train_image, template, user):
    if train_image is None:
        # 使用内建镜像
        if (await AioTrainEnvironmentSelector.find_one(env_name=template)) is None:
            return f'内建镜像 [{template}] 不存在, 请检查拼写'
        if template not in user.quota.train_environments:
            return f'用户没有使用内建镜像 [{template}] 的权限'
    else:
        if len(train_image.split('/')) != 3:
            return 'train_image 格式不正确, 请检查. 仅支持镜像 URL, 请参考 hfai client 文档.'
        # 使用自定义镜像
        valid_image_urls = await TrainImageSelector.a_find_user_group_image_urls(shared_group=user.shared_group, status='loaded')
        if train_image not in valid_image_urls:
            return f'用户所在的组 [{user.shared_group}] 不存在镜像 [{train_image}] 或镜像仍在加载, 请使用命令 `hfai images list` 检查'
    return None


async def create_task_base_queue(*args, **kwargs):
    return {
        'success': 1,
        'msg': 'not implemented',
        'task': {}
    }


async def check_sidecar_get_err(task_schema: TaskSchema, task: BaseTask):
    return None


async def process_create_task(task_schema: TaskSchema, task: BaseTask) -> Dict:
    return {
        'success': 1,
        'task': task
    }
