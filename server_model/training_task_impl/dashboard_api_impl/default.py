

from abc import ABC
from server_model.training_task_impl.additional_property_impl import AdditionalPropertyImpl


class DashboardApiImpl(AdditionalPropertyImpl, ABC):
    async def get_latest_point(self):
        return {
                'gpu_util': -1,
                'ib_recv': -1,
                'ib_trans': -1
            }

    async def get_chain_time_series(self, query_type: str, rank: int, *args, **kwargs):
        '''
        获取整条chain的数据
        '''
        return None
