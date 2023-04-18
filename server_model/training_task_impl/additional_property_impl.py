

from abc import ABC
from cached_property import cached_property

from base_model.training_task import ITrainingTaskImpl
from server_model.task_impl.single_task_impl import SingleTaskImpl


class AdditionalPropertyImpl(SingleTaskImpl, ITrainingTaskImpl, ABC):

    @cached_property
    def sys_environments(self, *args, **kwargs):
        res = {
            'MARSV2_WHOLE_LIFE_STATE': self.task.whole_life_state,
            'CHAIN_ID': self.task.chain_id,
            'IBV_FORK_SAFE': '1',
            # 暂时兼容老的
            'SINCE_FIRST_START': '0',
            **super(AdditionalPropertyImpl, self).sys_environments
        }
        return res
