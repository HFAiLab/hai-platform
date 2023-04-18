
from typing import Tuple, List, Optional

from base_model.task_selector import TaskSelector
from base_model.training_task import BaseTask
from db import MarsDB


class AioBaseTaskSelector(TaskSelector):
    @classmethod
    async def where(cls, impl_cls, where: str, args: Tuple, limit: int, order_desc: bool = True) -> list:
        """
        @note: 我只拿 1000 条实验

        @param where:
        @param args:
        @param impl_cls:
        @param limit:
        @param order_desc:
        @return:
        """
        sql = f'''
            select "t".*, coalesce("tt"."tags", '{{}}')::varchar[] as "tags"
            from (
                select "task_ng".*
                from "task_ng"
                where {where}
                group by "task_ng"."id"
                order by id {'desc' if order_desc else 'asc'}
                limit {limit}
            ) as "t"
            left join (
                select array_agg("task_tag"."tag") as "tags", "chain_id"
                from "task_tag"
                group by "chain_id"
            ) "tt" on "tt"."chain_id" = "t"."chain_id"
        '''
        # note: 因为我们的内存足够大，所以目前来说 filter 以及 page 操作都可以把个人用户的数据筛选出来再做
        results = await MarsDB().a_execute(sql, args)
        tasks = []
        for result in results:
            tasks.append(BaseTask(impl_cls, **result))
        return tasks

    @classmethod
    async def find_one(cls, impl_cls, **kwargs) -> Optional[BaseTask]:
        # get value from kwargs
        id = kwargs.get('id', None)
        chain_id = kwargs.get('chain_id', None)
        nb_name = kwargs.get('nb_name', None)
        user_name = kwargs.get('user_name', None)

        if id is not None:
            result = await cls.where(impl_cls, '"id" = %s', (int(id), ), limit=1)
        elif chain_id is not None:
            result = await cls.where(impl_cls, '"chain_id" = %s', (chain_id, ), limit=1)
        else:
            result = await cls.where(impl_cls, '"nb_name" = %s and "user_name" = %s', (nb_name, user_name), limit=1)
        if len(result) > 0:
            return result[0]
        else:
            return None

    @classmethod
    async def find_list(cls, impl_cls, **kwargs) -> List[BaseTask]:
        # get value from kwargs
        queue_status = kwargs.get('queue_status', None)
        user_name = kwargs.get('user_name', None)
        limit = kwargs.get('limit', 10000)
        chain_id = kwargs.get('chain_id', None)
        order_desc = kwargs.get('order_desc', None)
        assert order_desc is not None, '必须指定 order_desc'

        if queue_status is not None:
            return await cls.where(impl_cls, '"queue_status" = %s', (queue_status, ), limit=limit, order_desc=order_desc)
        elif user_name is not None:
            return await cls.where(impl_cls, '"user_name" = %s', (user_name, ), limit=limit, order_desc=order_desc)
        elif chain_id is not None:
            return await cls.where(impl_cls, '"chain_id" = %s', (chain_id, ), limit=limit, order_desc=order_desc)
        else:
            raise NotImplementedError

    @classmethod
    async def get_error_info(cls, id, *args, **kwargs) -> Optional[BaseTask]:  # 只在获取log时用到，不会用到同步的db
        sql = f'''
            select *
            from "task_error_info"
            where "id" = {id};
        '''
        results = await MarsDB().a_execute(sql)
        results = results.first()
        return results.error_info if results else ""
