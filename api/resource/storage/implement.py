
from .default import *
from .custom import *

from fastapi import Depends, HTTPException
from logm import logger
from pydantic import BaseModel, validator
from typing import List, Optional

from api.depends import get_api_user_with_token
from conf.flags import USER_ROLE
from db import MarsDB
from server_model.user import User
from server_model.user_data import UserWithAllGroupsTable
from server_model.selector import AioUserSelector


WARNING_NOTE = '(使用 force=true 忽略告警)'


class MountPoint(BaseModel):
    host_path: str
    mount_path: str
    owners: List[str]
    conditions: List[str]
    mount_type: str
    read_only: Optional[bool] = None

    @validator('owners')
    def ensure_single_owner(cls, v):
        if len(v) != 1:
            raise ValueError('owners 仅支持长度为1的列表, 如有需要可添加多条记录')
        return v

    @validator('conditions')
    def conditions_no_or(cls, vlist):
        for v in vlist:
            if ' or ' in v:
                raise ValueError('conditions 不允许使用 or 运算符, 如有需要可添加多条记录')
            if ' and ' in v:
                raise ValueError('conditions 不允许使用 and 运算符, 请通过拆分为多个 condition 的方式实现 and 逻辑')
        return vlist


async def get_user_storage_list(user: User = Depends(get_api_user_with_token())):
    """获取用户的可用挂载点路径"""
    return {
        'success': 1,
        'storages': await user.storage.async_get()
    }


async def _insert_mount_point(mount_point: MountPoint, action: str):
    sql = 'insert into "storage"("host_path", "mount_path", "owners", "conditions", "mount_type", "read_only", "action")' \
          'values (%s, %s, %s, %s, %s, %s, %s)'
    params = (
        mount_point.host_path, mount_point.mount_path, mount_point.owners, mount_point.conditions,
        mount_point.mount_type, mount_point.read_only, action)
    logger.info(f'插入挂载记录 {mount_point} action={action}')
    return await MarsDB().a_execute(sql, params)


async def _get_mount_points(mount_point: MountPoint, action: str = None):
    sql = 'select * from "storage"' \
          'where "host_path"=%s and "mount_path"=%s and %s && "owners"::text[] and "active"' \
          ' and "conditions"::text[] @> %s and "conditions"::text[] <@ %s'
    params = [mount_point.host_path, mount_point.mount_path, mount_point.owners, mount_point.conditions, mount_point.conditions]
    if action is not None:
        sql += ' and action = %s'
        params.append(action)
    return await MarsDB().a_execute(sql, tuple(params))


async def _delete_mount_points(mount_point: MountPoint, action: str = None):
    sql = 'delete from "storage"' \
          'where "host_path"=%s and "mount_path"=%s and %s && "owners"::text[] and "active"' \
          ' and "conditions"::text[] @> %s and "conditions"::text[] <@ %s'
    params = [mount_point.host_path, mount_point.mount_path, mount_point.owners, mount_point.conditions, mount_point.conditions]
    if action is not None:
        sql += ' and action = %s'
        params.append(action)
    logger.info(f'删除挂载记录 {mount_point} action={action}')
    return await MarsDB().a_execute(sql, tuple(params))


async def check_conflict(mount_point: MountPoint):
    """ 提前检查变更后可能存在的冲突, 即 同优先级存在多条 mount_path 相同的挂载记录的情况 """
    owner = mount_point.owners[0]
    mount_info = lambda m: f'[{m.host_path}]->[{m.mount_path}], owners={m.owners}, conditions={m.conditions}, action={m.action}'

    # 筛选同优先级、可能冲突的 conditions 条件
    conditions_where, conditions_params = \
        (''' "conditions" = '{}' ''', tuple()) if len(mount_point.conditions) == 0 else \
        ('("conditions"::text[] <@ %s or "conditions"::text[] @> %s)', (mount_point.conditions, mount_point.conditions))

    if (await AioUserSelector.find_one(user_name=owner)) is not None:
        # owner 是用户, 则查 owners 同为该用户、属于同一个优先级 rank 的挂载记录, 其在特定 conditions 会发生冲突
        sql = f'select * from "storage" where "mount_path" = %s and %s=any("owners") and "active" and {conditions_where}'
        records = await MarsDB().a_execute(sql, params=(mount_point.mount_path, owner, *conditions_params))
        if len(records := records.fetchall()) > 0:
            return f'将添加的挂载记录与以下记录冲突:\n' + '\n'.join(mount_info(r) for r in records)
    else:
        user_df = await UserWithAllGroupsTable.async_df
        # owner 是用户组, 先查其包含的所有用户
        users = user_df[user_df.user_groups.apply(lambda gs: owner in gs)].user_name.tolist()
        # 再查这些用户属于的所有组
        groups = user_df[user_df.user_name.apply(lambda uname: uname in users)].user_groups.explode().tolist()
        groups = list(set(groups))
        # 再查 owners 是这些组、属于同一个优先级 rank 的挂载记录, 其在特定 conditions 下会在特定用户身上发生冲突
        sql = f'select * from "storage" where "mount_path" = %s and "owners"::text[] && %s and "active" and {conditions_where}'
        records = await MarsDB().a_execute(sql, params=(mount_point.mount_path, groups, *conditions_params))
        if len(records := records.fetchall()) > 0:
            msg = '将添加的挂载记录与以下记录冲突:\n'
            for r in records:
                msg += mount_info(r) + ', '
                overlap_users = user_df[user_df.user_groups.apply(lambda gs: owner in gs and r.owners[0] in gs)].user_name.tolist()
                if len(overlap_users) > 5:
                    msg += '影响: ' + ','.join(overlap_users[:5]) + f'...(共{len(overlap_users)})\n'
                else:
                    msg += '影响: ' + ','.join(overlap_users) + '\n'
            return msg


async def create_mount_point(mount_point: MountPoint, force: bool = False,
                             user: User = Depends(get_api_user_with_token(allowed_groups=['ops', 'cluster_manager']))):
    if mount_point.read_only is None:
        raise HTTPException(status_code=400, detail='添加挂载点时必须指定 read_only')

    # 1. 尝试找到 action=remove 的挂载点, 之前若 remove 过, 本次的操作是撤销 remove 动作
    records = (await _get_mount_points(mount_point)).fetchall()
    if len(records) > 1:
        raise HTTPException(status_code=400,
                            detail='当前 (host_path, mount_path, owners, conditions) 对应多条挂载记录, 需要人工检查')
    elif len(records) > 0:
        # 指定参数存在挂载记录
        if records[0].action == 'add':
            raise HTTPException(status_code=400,
                                detail='当前 (host_path, mount_path, owners, conditions) 已存在 action=add 的挂载记录')
        await _delete_mount_points(mount_point, action='remove')
        return {
            'success': 1,
            'msg': '指定参数存在 action=remove 的挂载记录, 现已删除'
        }

    # 2. 指定参数不存在挂载记录, 添加之前进行校验
    if not force:
        user_or_group = mount_point.owners[0]
        user = await AioUserSelector.find_one(user_name=user_or_group)
        group = await MarsDB().a_execute('select 1 from "user_group" where "group" = %s', (user_or_group,))
        if user is None and len(group.fetchall()) == 0 and user_or_group not in {'public', USER_ROLE.INTERNAL, USER_ROLE.EXTERNAL}:
            return {'success': 0, 'msg': f'Warning: 指定的 owner [{user_or_group}] 不是现有的用户名或用户组. ' + WARNING_NOTE}
        if user is not None:
            # 指定用户时, 检查是否已经通过用户组获得了挂载
            target_mount = mount_point.copy(update={'owners': user.group_list})
            records = await _get_mount_points(mount_point=target_mount, action='add')
            if (record := records.fetchone()) is not None:
                return {'success': 0, 'msg': f'Warning: 指定用户 {user.user_name} 已经通过 {record.owners[0]} 组获得了该挂载点. ' + WARNING_NOTE}
        if (err_msg := await security_check(mount_point)) is not None:     # 安全相关的检验
            return {'success': 0, 'msg': f'Warning: 安全检查未通过 {WARNING_NOTE}:\n{err_msg}'}
        if (err_msg := await check_conflict(mount_point)) is not None:
            return {'success': 0, 'msg': err_msg}

    try:
        await _insert_mount_point(mount_point=mount_point, action='add')
    except Exception as e:
        logger.exception(e)
        return {'success': 0, 'msg': f'添加 action=add 的挂载记录失败: {e}'}
    return {
        'success': 1,
        'msg': '添加 action=add 的挂载记录成功'
    }


async def delete_mount_point(mount_point: MountPoint, force: bool = False,
                             user: User = Depends(get_api_user_with_token(allowed_groups=['ops', 'cluster_manager']))):
    # 0. 含有通配符时可以直接写入
    if '*' in mount_point.host_path or '*' in mount_point.mount_path:
        try:
            await _insert_mount_point(mount_point=mount_point, action='remove')
        except Exception as e:
            logger.exception(e)
            return {'success': 0, 'msg': f'直接添加含有通配符的挂载记录失败: {e}'}
        return {
            'success': 1,
            'msg': '添加 action=remove 的挂载记录成功'
        }

    # 1. 尝试找到 action=add 的挂载点, 之前若 add 过, 本次的操作是撤销 add 动作
    records = (await _get_mount_points(mount_point, action='add')).fetchall()
    if len(records) > 0:
        await _delete_mount_points(mount_point, action='add')
        return {
            'success': 1,
            'msg': '找到了 action=add 的挂载记录并删除成功'
        }

    # 2. 之前没有 add 过, 但 owner 是用户时, 先检查其用户组是否挂载了这个 host_path, 尽量避免无意义的 remove 动作
    if not force and (user := await AioUserSelector.find_one(user_name=mount_point.owners[0])) is not None:
        target_mount = mount_point.copy(update={'owners': user.group_list})
        records = await _get_mount_points(target_mount, action='add')
        if records.fetchone() is None:
            return {'success': 0, 'msg': f'Warning: 指定用户 {user.user_name} 在指定 condition 下没有该挂载点. ' + WARNING_NOTE}

    # 3. 添加 remove 动作
    # 检查冲突
    if not force and (err_msg := await check_conflict(mount_point)) is not None:
        return {'success': 0, 'msg': err_msg}

    try:
        await _insert_mount_point(mount_point=mount_point, action='remove')
    except Exception as e:
        logger.exception(e)
        return {'success': 0, 'msg': f'添加 action=remove 的挂载记录失败 {e}'}
    return {'success': 1, 'msg': '添加 action=remove 的挂载记录成功'}
