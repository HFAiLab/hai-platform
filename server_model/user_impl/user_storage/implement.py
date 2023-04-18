
from .default import *
from .custom import *

import tokenize
import uuid
from collections import defaultdict
from io import BytesIO
from itertools import chain, combinations
from functools import cached_property

import pandas as pd
from sqlalchemy import text

from base_model.base_user_modules import IUserStorage
from db import MarsDB
from server_model.user import User
from server_model.user_data import AllStorageTable


class STORAGE_ACTION:
    ADD = 'add'
    REMOVE = 'remove'


def _merge_by_condition(condition_to_values: dict):
    """
        根据条件合并 values 列表.
        (例如, 满足 [A==1, B==1] 条件则必定同时满足 [], [A==1], [B==1] 三种条件,
            因此若条件 [A==1, B==1] 的子集有对应的 values, 则其应该合并入 [A==1, B==1] 条件对应的 values list)
    """
    merged = condition_to_values.copy()
    for condition, values in merged.items():
        for condition_combo in chain(*[combinations(condition, r) for r in range(len(condition))]):
            condition_key = tuple(sorted(list(condition_combo)))
            if condition_key in condition_to_values:
                values += condition_to_values[condition_key]
    return merged


def _build_mount_point(mount_point):
    host_path, mount_path, mount_type, read_only = mount_point
    return {'host_path': host_path, 'mount_path': mount_path, 'mount_type': mount_type, 'read_only': read_only}


class UserStorage(UserStorageExtras, IUserStorage):
    def __init__(self, user: User):
        IUserStorage.__init__(self, user)
        self.user: User = user
        self._storage = None

    # 注意 k8sworker 复用了这个逻辑，改动的时候要小心
    def safe_eval(self, s, task=None):
        # tokenize，判断输入是否合法
        g = tokenize.tokenize(BytesIO(s.encode('utf-8')).readline)
        tokens = []
        for tokenum, tokval, _, _, _ in g:
            try:
                assert tokenum in {
                    tokenize.NUMBER, tokenize.STRING, tokenize.OP, tokenize.NAME, tokenize.ENCODING,
                    tokenize.ENDMARKER, tokenize.NEWLINE
                }, f'token 非法: {tokval}'
                if tokenum == tokenize.NAME:
                    if tokval not in {'and', 'or', 'is', 'None', 'task', 'user', 'startswith', 'endswith'}:
                        # 如果不在这里，那必须之前是 task. / user.
                        assert tokens[-1] == '.', f'当前 token 是 {tokval}，前一个必须是 .'
                        assert tokens[-2] in {'task', 'user'}, f'当前 token 是 {tokval}，前前一个必须是 task / user'
            except Exception as e:
                # token 有问题，这条作废
                return
            tokens.append(tokval)
        try:
            # 正常，直接 eval
            return eval(s, {'task': task, 'user': self.user})
        except Exception as e:
            # eval 出错了，这条作废
            return

    def process_storage(self, df):
        # 解析 storage，按照以下规则排序
        # rank0 有 condition 用户挂载点 (最优先)
        # rank1 无 condition 用户挂载点
        # rank2 有 condition 用户组挂载点
        # rank3 无 condition 用户组挂载点
        # rank 已经在 sql 操作中打好了，但是 sql 比较难做取交集的操作
        user_group_set = set(self.user.db_str_group_list.replace("'", '').split(','))
        df['owners'] = df.owners.apply(lambda o: list(set(o) & user_group_set))
        return df

    def storage_sql(self, where=None):
        if where is None:
            where = f'where owners && array[{self.user.db_str_group_list}]::varchar[]'
        # 这里来 sort，可以看看 process_storage
        return f'''
            select
                "host_path", "mount_path", "owners", "conditions",
                "mount_type", "read_only", "action", "rank", "need_task"
            from (
                 select
                    "host_path", "mount_path", "owners", "conditions",
                    "mount_type", "read_only", "action", "active",
                    case
                        when "owners" @> array['{self.user.user_name}']::varchar[] and "conditions" != array[]::varchar[] then 0
                        when "owners" @> array['{self.user.user_name}']::varchar[] and "conditions" = array[]::varchar[] then 1
                        when not "owners" @> array['{self.user.user_name}']::varchar[] and "conditions" != array[]::varchar[] then 2
                        else 3
                    end as "rank",
                    case
                        when "host_path" like '%{{task.%' then true
                        when "mount_path" like '%{{task.%' then true
                        when "conditions"::text like '%task.%' then true
                        else false
                    end as "need_task"
                from "storage"
                {where}
            ) as "tmp"
            where "active"
            order by "rank" DESC 
        '''

    async def create_storage_df(self):
        if self._storage is None:
            results = await MarsDB().a_execute(self.storage_sql())
            self._storage = pd.DataFrame.from_records([{**r} for r in results])
            self._storage = self.process_storage(self._storage)
        return self._storage

    @cached_property
    def storage_df(self):
        if self._storage is None:
            self._storage = pd.read_sql(text(self.storage_sql()), MarsDB().db)
            self._storage = self.process_storage(self._storage)
        return self._storage

    def personal_storage(self, task=None):
        if task is None:
            # 没有 task，就把 need_task 的挂载点给删了
            storage_df = self.storage_df[~self._storage.need_task].copy()
        else:
            storage_df = self.storage_df.copy()
        res = {}
        # 依次遍历每一个 rank
        for _, df in storage_df.groupby('rank', sort=False):
            rank_res = {}
            dup_mounts = []
            # 先执行 remove
            for _, row in df[df.action == STORAGE_ACTION.REMOVE].iterrows():
                if not all(self.safe_eval(c, task=task) for c in row.conditions):
                    continue
                mount_path = self.safe_eval(f"f'{row.mount_path}'", task=task)
                host_path = self.safe_eval(f"f'{row.host_path}'", task=task)
                # 解析出问题了，直接跳过
                if mount_path is None:
                    continue
                if host_path.endswith('*'):  # 通配，用于把 /3fs* 的挂载点全部去掉
                    for p in list(res.keys()):
                        if res[p]['host_path'].startswith(host_path[:-1]):
                            res.pop(p)
                if mount_path.endswith('*'):
                    for p in list(res.keys()):
                        if p.startswith(mount_path[:-1]):
                            res.pop(p)
                res.pop(mount_path, None)
            # 再执行 add
            for _, row in df[df.action == STORAGE_ACTION.ADD].iterrows():
                if not all(self.safe_eval(c, task=task) for c in row.conditions):
                    continue
                mount_path = self.safe_eval(f"f'{row.mount_path}'", task=task)
                # 解析出问题了，直接跳过
                if mount_path is None:
                    continue
                # 出错了，同一个 rank 有相同的 mount_path 定义
                if rank_res.get(mount_path):
                    dup_mounts.append(mount_path)
                host_path = self.safe_eval(f"f'{row.host_path}'", task=task)
                # 解析出问题了，直接跳过
                if host_path is None:
                    continue
                rank_res[mount_path] = {
                    'host_path': host_path,
                    'mount_path': mount_path,
                    'mount_type': row.mount_type,
                    'read_only': row.read_only,
                    'name': uuid.uuid4().hex
                }
            # 同一个 rank 有重复的，直接删了
            res = {**res, **{k: v for k, v in rank_res.items() if k not in dup_mounts}}
        return list(res.values())

    async def get_user_storage_info(self):
        storage_df = await self.create_storage_df()
        deduction_items = defaultdict(lambda: defaultdict(list))
        for _, row in storage_df.iterrows():
            mount_point = (row.host_path, row.mount_path, row.mount_type, row.read_only)
            condition = tuple(sorted(row.conditions))
            deduction_items[mount_point][condition].append({
                'priority': 3-row['rank'],
                'hit_groups': row.owners,
                'action': row.action,
                'conditions': list(condition),
            })

        results = []
        for mount_point, condition_to_deductions in deduction_items.items():
            condition_to_deductions = _merge_by_condition(condition_to_deductions)
            possible_statuses = []
            for condition, deduction_items in condition_to_deductions.items():
                # 高优先级优先, 同级 remove 优先
                deduction_items.sort(key=lambda x: (x['priority'], x['action'] == STORAGE_ACTION.REMOVE), reverse=True)
                final_status = 'enabled' if deduction_items[0]['action'] == STORAGE_ACTION.ADD else 'disabled'
                possible_statuses.append({
                    'status': final_status,
                    'conditions': list(condition),
                    'deduction_details': deduction_items
                })
            mount_item = {
                'mount_point': _build_mount_point(mount_point),
                'possible_statuses': sorted(possible_statuses, key=lambda x:len(x['conditions']), reverse=True)
            }
            results.append(mount_item)
        return results

    @classmethod
    async def get_all_storage_df(cls):
        return await AllStorageTable.async_df

    @classmethod
    async def get_all_storage_info(cls):
        storage_df = await cls.get_all_storage_df()
        mount_to_rows = defaultdict(lambda: defaultdict(list))
        for _, row in storage_df.iterrows():
            mount_point = (row.host_path, row.mount_path, row.mount_type, row.read_only)
            condition = tuple(sorted(row.conditions))
            mount_to_rows[mount_point][condition].append(row)

        results = []
        for mount_point, condition_to_rows in mount_to_rows.items():
            # 处理每个 mount point
            conditions = [
                {'type': row.action, 'conditions': row.conditions, 'groups': row.owners}
                for row in chain(*condition_to_rows.values())
            ]
            condition_to_rows = _merge_by_condition(condition_to_rows)
            accessible_users_all_conditions = []
            for condition, rows in condition_to_rows.items():
                # 计算当前条件下的 accessible users
                accessible_users = []
                user_to_groups = defaultdict(lambda :defaultdict(list))
                for row in rows:
                    for group, user in row.affected_users:
                        # 线上逻辑优先级: [用户挂载点 > 组挂载点] -> [有 condition > 无 conditoin] -> [remove > add]
                        priority = (user == group, len(row.conditions) > 0, row.action==STORAGE_ACTION.REMOVE)
                        # 按优先级聚合命中的权限组
                        user_to_groups[user][priority].append(group)
                for user, priority_to_groups in user_to_groups.items():
                    (_, _, is_remove), groups = max(priority_to_groups.items(), key=lambda k: k[0]) # 取最高优先级
                    if not is_remove: # 最高优先级规则是 add 时, 认为用户可访问挂载点
                        accessible_users.append({'user': user, 'hit_groups': groups})
                accessible_users_all_conditions.append({
                    'conditions': list(condition),
                    'users': accessible_users
                })
            results.append({
                'mount_point': _build_mount_point(mount_point),
                'conditions': conditions,
                'accessible_users': accessible_users_all_conditions,
            })
        return results
