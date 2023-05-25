
from .default import *
from .custom import *

from itertools import chain
from typing import Optional, List, Union

import pandas as pd
from fastapi import Depends, HTTPException
from kubernetes.client import ApiException
from pydantic import BaseModel

from api.depends import get_api_user_with_token, get_internal_api_user_with_token
from conf import MARS_GROUP_FLAG, CONF
from conf.flags import MOUNT_CODE
from db import MarsDB
from k8s.async_v1_api import async_get_nodes_df, async_read_node, async_set_node_label
from server_model.user import User


async def cluster_df(monitor: bool = True, user: User = Depends(get_api_user_with_token())):
    """
    获取集群的状态，接口返回 dataframe
    @return:
    """
    df = await async_get_nodes_df(monitor=monitor)
    await user.quota.create_quota_df()
    df = post_process_cluster_df(df, user)
    return dict(
        success=1,
        cluster_df=df.to_dict('records'),
        containers=user.quota.train_environments,
        mount_code=MOUNT_CODE
    )


async def change_node_state_api(node_name: str, state: str, user: User = Depends(
    get_api_user_with_token(allowed_groups=['cluster_manager', 'ops']))):
    try:
        node = await async_read_node(node_name)
    except ApiException as e:
        if e.status == 404:
            raise HTTPException(404, detail=f'要操作的节点 [{node_name}] 不存在')
        else: raise HTTPException(400, detail=f'读取节点信息失败') from e
    if state not in ['enabled', 'disabled']:
        raise HTTPException(400, detail=f'不合法的目标状态 [{state}]')

    err_group = CONF.scheduler.error_node_meta_group
    group_prefix = f'{err_group}.manually_disabled_by_'
    mars_group = node.metadata.labels[MARS_GROUP_FLAG]
    if state == 'disabled':
        if mars_group.startswith(err_group):
            raise HTTPException(400, detail=f'节点 [{node_name}] 未启用 (group={mars_group})')
        new_group = group_prefix + user.user_name
    else:   # enabled
        if not mars_group.startswith(group_prefix):
            raise HTTPException(400, detail=f'节点 [{node_name}] 未通过本接口禁用 (group={mars_group})')
        origin_group = (await MarsDB().a_execute('select "origin_group" from "host" where node = %s', (node_name, ))).fetchone()[0]
        new_group = origin_group
    res = await async_set_node_label(node_name, MARS_GROUP_FLAG, new_group)
    return {
        'success': res,
        'msg': f'设置节点状态为{state}' + '成功' if res else '失败'
    }


class HostInfo(BaseModel):
    node: str
    gpu_num: int = None
    type: str = None
    use: str = None
    origin_group: str = None
    room: str = None
    schedule_zone: str = None


async def update_host_info_api(node: HostInfo, user: User = Depends(
        get_api_user_with_token(allowed_groups=['cluster_manager', 'ops']))):
    update_attrs = {k: v for k, v in node.dict().items() if v is not None and k != 'node'}
    if len(update_attrs) == 0:
        raise HTTPException(status_code=400, detail=f'必须至少指定一个要更新的属性')
    assigns = ','.join(f'"{k}"=%s' for k in update_attrs)
    sql = f'update "host" set {assigns} where "node" = %s'
    if (await MarsDB().a_execute(sql, (*update_attrs.values(), node.node))).rowcount == 0:
        raise HTTPException(404, detail=f'未找到指定的节点 [{node.node}]')
    return {
        'success': 1,
        'msg': '更新成功'
    }


async def delete_host_info_api(node: str, user: User = Depends(
        get_api_user_with_token(allowed_groups=['cluster_manager', 'ops']))):
    sql = 'delete from "host" where "node" = %s'
    if (await MarsDB().a_execute(sql, (node,))).rowcount == 0:
        raise HTTPException(404, detail=f'未找到指定的节点 [{node}]')
    return {
        'success': 1,
        'msg': '删除成功'
    }


async def create_host_info_api(node: HostInfo, user: User = Depends(
        get_api_user_with_token(allowed_groups=['cluster_manager', 'ops']))):
    nodes = [node]  # 暂时只支持单条处理
    for node in nodes:
        if any([v is None for v in node.dict().values()]):
            raise HTTPException(status_code=400, detail=f'创建 host info 时必须指定全部属性')

    sql = 'select node from "host" where "node" in (' + ','.join(['%s'] * len(nodes)) + ')'
    if len(dups := (await MarsDB().a_execute(sql=sql, params=tuple(node.node for node in nodes))).fetchall()) > 0:
        return {'success': 0, 'msg': f'以下节点已经存在: {[dup.node for dup in dups]}'}

    sql = 'insert into "host"("node", "gpu_num", "type", "use", "origin_group", "room", "schedule_zone") values ' \
        + ','.join([f'(%s, {int(node.gpu_num)}, %s, %s, %s, %s, %s)' for node in nodes])
    params = tuple(chain.from_iterable(
        [node.node, node.type, node.use, node.origin_group, node.room, node.schedule_zone] for node in nodes))
    await MarsDB().a_execute(sql=sql, params=params)
    return {'success': 1, 'msg': '创建成功'}


async def get_host_info_api(node_regex: str = '', type: str = None, use: str = None, origin_group: str = None,
                            schedule_zone: str = None,
                            user: User = Depends(get_internal_api_user_with_token())):
    filter = {'type': type, 'use': use, 'origin_group': origin_group, 'schedule_zone': schedule_zone}
    where = ' '.join(f'and {k} = %s' for k in filter if filter[k] is not None)
    params = [node_regex] + list(v for v in filter.values() if v is not None)
    sql = f'''
        select "node", "gpu_num", "type", "use", "origin_group", "schedule_zone"
        from "host" where "node" ~ %s {where}
    '''
    rows = (await MarsDB().a_execute(sql, tuple(params))).fetchall()
    return {
        'success': 1,
        'result': {
            'data': [dict(row) for row in rows]
        }
    }


async def label_node_api(node_name: str, label: str, user: User = Depends(get_api_user_with_token(allowed_groups=['cluster_manager', 'ops']))):
    try:
        await async_read_node(node_name)
    except ApiException as e:
        if e.status == 404:
            raise HTTPException(404, detail=f'要操作的节点 [{node_name}] 不存在')
        else: raise HTTPException(400, detail=f'读取节点信息失败') from e
    res = await async_set_node_label(node_name, MARS_GROUP_FLAG, label)
    return {
        'success': res,
        'msg': f'设置节点标签为{label}' + '成功' if res else '失败'
    }
