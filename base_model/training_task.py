from typing import Tuple, Optional

from cached_property import cached_property

from .base_task import BaseTask, ITaskImpl
from .mini_traits import List, Str

try:
    from conf.flags import EXP_STATUS, QUE_STATUS, SUSPEND_CODE, STOP_CODE, \
        TASK_TYPE, CHAIN_STATUS, TASK_FLAG
except :
    from hfai.conf.flags import EXP_STATUS, QUE_STATUS, SUSPEND_CODE, STOP_CODE, \
        TASK_TYPE, CHAIN_STATUS, TASK_FLAG


class TrainingTask(BaseTask):
    """
    训练任务
    """
    id_list: list = List()
    queue_status_list: list = List()
    begin_at_list: list = List()
    end_at_list: list = List()
    stop_code_list: list = List()
    suspend_code_list: list = List()
    whole_life_state_list: list = List()
    created_at_list: list = List()
    worker_status_list: list = List()
    chain_status: str = Str()

    def __init__(self, implement_cls=None, **kwargs):
        super().__init__(implement_cls, **kwargs)
        self.__impl__: Optional[ITrainingTaskImpl] = None
        self.whole_life_state = int(self.whole_life_state_list[-1]) if len(self.whole_life_state_list) else 0
        self.stop_code = int(self.stop_code_list[-1]) if len(self.stop_code_list) else 0
        self.suspend_code = int(self.suspend_code_list[-1]) if len(self.suspend_code_list) else 0
        self.star = 'star' in self.tags


    @BaseTask._bind_impl_
    async def log(self, rank: int, last_seen=None, *args, **kwargs):
        """获取log的方法"""
        return await self.__impl__.log(rank, last_seen, *args, **kwargs)

    @BaseTask._bind_impl_
    async def log_ng(self, rank: int = 0, last_seen=None, *args, **kwargs):
        """
        获取训练任务日志

        Args:
            rank (int): 节点编号
            last_seen (str, optional): 上次读取到的日志的last_seen，用于断点续读，默认为None

        Returns:
            返回一个dict，其中data表示日志，last_seen表示本次读取到的日志位置

        Examples:

            >>> import asyncio
            >>> asyncio.run(experiment.log_ng(rank=0))

        """

        return await self.__impl__.log_ng(rank, last_seen, *args, **kwargs)

    @BaseTask._bind_impl_
    async def sys_log(self, *args, **kwargs):
        """获取sys log的方法"""
        return await self.__impl__.sys_log(*args, **kwargs)

    @BaseTask._bind_impl_
    async def search_in_global(self, content, *args, **kwargs):
        """全局搜索该任务每个rank包含content的次数"""
        return await self.__impl__.search_in_global(content, *args, **kwargs)

    @BaseTask._bind_impl_
    async def star_task(self, *args, **kwargs):
        """使任务为星标任务"""
        return await self.__impl__.star_task(*args, **kwargs)

    @BaseTask._bind_impl_
    async def unstar_task(self, *args, **kwargs):
        """解除星标任务"""
        return await self.__impl__.unstar_task(*args, **kwargs)

    @BaseTask._bind_impl_
    async def tag_task(self, tag: str, *args, **kwargs):
        return await self.__impl__.tag_task(tag, *args, **kwargs)

    @BaseTask._bind_impl_
    async def untag_task(self, tag: str, *args, **kwargs):
        return await self.__impl__.untag_task(tag, *args, **kwargs)

    @BaseTask._bind_impl_
    async def map_task_artifact(self, artifact_name: str, artifact_version: str, direction: str, *args, **kwargs):
        return await self.__impl__.map_task_artifact(artifact_name, artifact_version, direction, *args, **kwargs)

    @BaseTask._bind_impl_
    async def unmap_task_artifact(self, direction: str, *args, **kwargs):
        return await self.__impl__.unmap_task_artifact(direction, *args, **kwargs)

    @BaseTask._bind_impl_
    async def get_task_artifact(self, *args, **kwargs):
        return await self.__impl__.get_task_artifact(*args, **kwargs)

    @BaseTask._bind_impl_
    async def stop(self, *args, **kwargs):
        """
        停止训练任务

        Args:

        Returns:
            None

        Examples:

            >>> import asyncio
            >>> asyncio.run(experiment.stop())

        """
        return await self.__impl__.stop(*args, **kwargs)

    @BaseTask._bind_impl_
    async def suspend(self, restart_delay: int = 0, *args, **kwargs):
        """
        打断训练任务

        Args:

        Returns:
            None

        Examples:

            >>> import asyncio
            >>> asyncio.run(experiment.suspend())

        """
        return await self.__impl__.suspend(restart_delay, *args, **kwargs)

    @BaseTask._bind_impl_
    def resume(self, *args, **kwargs):
        return self.__impl__.resume(*args, **kwargs)

    @BaseTask._bind_impl_
    async def get_latest_point(self, *args, **kwargs):
        return await self.__impl__.get_latest_point(*args, **kwargs)

    @BaseTask._bind_impl_
    async def get_chain_time_series(self, query_type: str, rank: int, *args, **kwargs):
        return await self.__impl__.get_chain_time_series(query_type, rank, *args, **kwargs)


class ITrainingTaskImpl(ITaskImpl):
    def __init__(self, task: TrainingTask):
        super().__init__(task)
        self.task: TrainingTask = task

    def update_pod_status(self, rank, status, *args, **kwargs):
        raise NotImplementedError

    def select_pods(self, *args, **kwargs):
        raise NotImplementedError

    async def log(self, rank: int, last_seen=None, *args, **kwargs):
        raise NotImplementedError

    async def log_ng(self, rank: int = 0, last_seen=None, *args, **kwargs):
        raise NotImplementedError

    async def sys_log(self, *args, **kwargs):
        raise NotImplementedError

    async def search_in_global(self, *args, **kwargs):
        raise NotImplementedError

    async def star_task(self, star, *args, **kwargs):
        raise NotImplementedError

    async def unstar_task(self, star, *args, **kwargs):
        raise NotImplementedError

    async def tag_task(self, tag, *args, **kwargs):
        raise NotImplementedError

    async def untag_task(self, tag, *args, **kwargs):
        raise NotImplementedError

    async def map_task_artifact(self, artifact_name: str, artifact_version: str, direction: str, *args, **kwargs):
        raise NotImplementedError

    async def unmap_task_artifact(self, direction: str, *args, **kwargs):
        raise NotImplementedError

    async def get_task_artifact(self, *args, **kwargs):
        raise NotImplementedError

    async def stop(self, *args, **kwargs):
        raise NotImplementedError

    async def suspend(self, restart_delay: int, *args, **kwargs):
        raise NotImplementedError

    async def resume(self, *args, **kwargs):
        raise NotImplementedError

    async def get_latest_point(self, *args, **kwargs):
        raise NotImplementedError

    async def get_chain_time_series(self, query_type: str, rank: int, *args, **kwargs):
        raise NotImplementedError

    def create(self, *args, **kwargs):
        raise NotImplementedError

    def create_error_info(self, failed_msg, *args, **kwargs):
        raise NotImplementedError

    def update_config_json_by_path(self, path, value, *args, **kwargs):
        raise NotImplementedError

    async def aio_update_config_json_by_path(self, path, value, *args, **kwargs):
        raise NotImplementedError

    def update(self, fields: Tuple[str], values: Tuple, *args, **kwargs):
        raise NotImplementedError

    @cached_property
    def user(self, *args, **kwargs):
        raise NotImplementedError
