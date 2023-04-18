

from typing import Tuple, List

from base_model.training_task import TrainingTask
from db import MarsDB
from server_model.selector.base_task_selector import BaseTaskSelector


class TrainingTaskSelector(BaseTaskSelector):
    @classmethod
    def where(cls, impl_cls, where: str, args: Tuple, limit: int, order_desc: bool = True) -> List[TrainingTask]:
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
        select
            "task_ng".*,
            "tmp_ng"."id_list",
            "tmp_ng"."queue_status_list",
            "tmp_ng"."begin_at_list",
            "tmp_ng"."end_at_list",
            "tmp_ng"."stop_code_list",
            "tmp_ng"."suspend_code_list",
            "tmp_ng"."whole_life_state_list",
            "tmp_ng"."worker_status_list",
            "tmp_ng"."created_at_list",
            coalesce("tt"."tags", '{{}}')::varchar[] as "tags"
        from
            "task_ng"
        inner join (
            select
                max("id")                                   as "id",
                array_agg("id" order by "id")               as "id_list",
                array_agg("queue_status" order by "id")     as "queue_status_list",
                array_agg("begin_at" order by "id")         as "begin_at_list",
                array_agg("end_at" order by "id")           as "end_at_list",
                array_agg("stop_code" order by "id")        as "stop_code_list",
                array_agg("suspend_code" order by "id")     as "suspend_code_list",
                array_agg("whole_life_state" order by "id") as "whole_life_state_list",
                array_agg("worker_status" order by "id")    as "worker_status_list",
                array_agg("created_at" order by "id")       as "created_at_list"
            from
                "task_ng"
            inner join (
                    select "chain_id", max("id")
                    from "task_ng"
                    where {where}
                    group by "chain_id"
                    order by max("id") {'desc' if order_desc else 'asc'}
                    limit {limit}
                ) as "tmp" on "tmp"."chain_id"="task_ng"."chain_id"
            group by "task_ng"."chain_id"
        ) as "tmp_ng" on "task_ng"."id"="tmp_ng"."id"
        left join (
            select array_agg("task_tag"."tag") as "tags", "chain_id"
            from "task_tag"
            group by "chain_id"
        ) "tt" on "tt"."chain_id" = "task_ng"."chain_id"
        order by "id" {'desc' if order_desc else 'asc'}
        '''
        # note: 因为我们的内存足够大，所以目前来说 filter 以及 page 操作都可以把个人用户的数据筛选出来再做
        results = MarsDB().execute(sql, args)
        tasks = []
        for result in results:
            tasks.append(TrainingTask(impl_cls, **result))
        return tasks

    @classmethod
    def find_one_by_id(cls, impl_cls, id) -> TrainingTask:
        """
        获取对应id的TrainingTask
        """
        sql = f'''
            select *
            from "task_ng"
            where "id" = %s;
        '''
        results = MarsDB().execute(sql, (id,))
        for result in results:
            return TrainingTask(impl_cls, **result)
