

import copy
from collections import defaultdict

from fastapi import Depends

from api.depends import get_api_user_with_token
from conf.flags import USER_ROLE, ALL_USER_ROLES
from k8s.async_v1_api import async_get_nodes_df
from server_model.user import User


async def get_train_images(user: User = Depends(get_api_user_with_token())):
    """ 获取内部的 train_image, 有两个来源，一个是用户组自己的 images，另一个是内建的 train_environment 表 """
    return {
        'success': 1,
        'result': await user.image.async_get()
    }


async def get_nodes_detail_api(user: User = Depends(get_api_user_with_token())):
    resource_df = await async_get_nodes_df(monitor=True)
    return {
        'success': 1,
        'result': resource_df.to_dict('records')
    }


def get_node_type_template():
    count_template = {
        'total': 0,
        'service': 0,
        'dev_and_release': {
            'total': 0,
            'release': 0,
            'dev': 0,
        },
        'train': {
            'total': 0,
            'schedulable': {
                'total': 0,
                'free': 0,
                'working': 0,
                **{f'{role}_working': 0 for role in ALL_USER_ROLES}
            },
            'unschedulable': 0,
        },
        'err': 0,
        'exclusive': 0,
    }
    return {
        'count': copy.deepcopy(count_template),
        'count_schedule_zone': defaultdict(lambda: copy.deepcopy(count_template)),
        'detail': {
            'err': defaultdict(int),
            'service': defaultdict(int),
            'exclusive': defaultdict(int),
            'train': {
                'working': {role: defaultdict(int) for role in ALL_USER_ROLES},
                'free': defaultdict(int),
                'unschedulable': defaultdict(int),
            }
        }
    }


# todo: 这个接口需要删掉，逻辑写到前端更自然
def get_nodes_overview_impl(nodes: list, for_monitor: bool):
    overview = defaultdict(lambda: get_node_type_template())
    for node in nodes:
        overview[node['type']]['count']['total'] += 1
        overview[node['type']]['count_schedule_zone'][node['schedule_zone']]['total'] += 1
        if node['current_category'] == 'service':
            overview[node['type']]['count']['service'] += 1
            overview[node['type']]['count_schedule_zone'][node['schedule_zone']]['service'] += 1
            overview[node['type']]['detail']['service'][node['use']] += 1
        if node['current_category'] in ['release', 'dev']:
            overview[node['type']]['count']['dev_and_release']['total'] += 1
            overview[node['type']]['count_schedule_zone'][node['schedule_zone']]['dev_and_release']['total'] += 1
            if node['current_category'] == 'release':
                overview[node['type']]['count']['dev_and_release']['release'] += 1
                overview[node['type']]['count_schedule_zone'][node['schedule_zone']]['dev_and_release']['release'] += 1
            else:
                overview[node['type']]['count']['dev_and_release']['dev'] += 1
                overview[node['type']]['count_schedule_zone'][node['schedule_zone']]['dev_and_release']['dev'] += 1
        if node['current_category'] == 'training':
            overview[node['type']]['count']['train']['total'] += 1
            overview[node['type']]['count_schedule_zone'][node['schedule_zone']]['train']['total'] += 1
            if node['status'] == 'Ready':
                overview[node['type']]['count']['train']['schedulable']['total'] += 1
                overview[node['type']]['count_schedule_zone'][node['schedule_zone']]['train']['schedulable']['total'] += 1
                if node['working'] is None:
                    overview[node['type']]['count']['train']['schedulable']['free'] += 1
                    overview[node['type']]['count_schedule_zone'][node['schedule_zone']]['train']['schedulable']['free'] += 1
                    overview[node['type']]['detail']['train']['free'][node['group']] += 1
                else:
                    overview[node['type']]['count']['train']['schedulable']['working'] += 1
                    overview[node['type']]['count_schedule_zone'][node['schedule_zone']]['train']['schedulable']['working'] += 1
                    if (role := node['working_user_role']) in ALL_USER_ROLES:
                        overview[node['type']]['count']['train']['schedulable'][f'{role}_working'] += 1
                        overview[node['type']]['count_schedule_zone'][node['schedule_zone']]['train']['schedulable'][f'{role}_working'] += 1
                        overview[node['type']]['detail']['train']['working'][role][node['working_user']] += 1
            else:
                overview[node['type']]['count']['train']['unschedulable'] += 1
                overview[node['type']]['count_schedule_zone'][node['schedule_zone']]['train']['unschedulable'] += 1
                overview[node['type']]['detail']['train']['unschedulable'][node['group']] += 1
        if node['current_category'] == 'err':
            overview[node['type']]['count']['err'] += 1
            overview[node['type']]['count_schedule_zone'][node['schedule_zone']]['err'] += 1
            overview[node['type']]['detail']['err'][node['group']] += 1
        if node['current_category'] == 'exclusive':
            overview[node['type']]['count']['exclusive'] += 1
            overview[node['type']]['count_schedule_zone'][node['schedule_zone']]['exclusive'] += 1
            overview[node['type']]['detail']['exclusive'][node['group']] += 1
    if not for_monitor:
        to_ret = overview['gpu']['count']
        # 公开的展示接口，隐藏调度细节
        for role in ALL_USER_ROLES:
            del to_ret['train']['schedulable'][f'{role}_working']
        return to_ret
    return overview


async def get_nodes_overview_api(user: User = Depends(get_api_user_with_token())):
    nodes = (await get_nodes_detail_api(user))['result']
    return {
        'success': 1,
        'result': {
            'nodes': nodes,
            'overview': get_nodes_overview_impl(nodes, True)
        }
    }


async def get_cluster_overview_for_client_api(user: User = Depends(get_api_user_with_token())):
    try:
        nodes = (await get_nodes_detail_api(user))['result']
        base = get_nodes_overview_impl(nodes, True)
        types = ('cpu', 'gpu')
        ret = {t: {} for t in types}
        for typ in types:
            if typ not in base:
                ret[typ] = {t: 0 for t in ('other', 'usage_rate', 'total', 'working', 'free')}
                continue
            data = base[typ]['count']
            others_count = data['dev_and_release']['total'] + data['err'] + data['train']['unschedulable'] + data['service'] + data['exclusive']
            usage_rate = 0 if data['train']['total'] == 0 else (data['train']['total'] - data['train']['schedulable']['free'] ) / data['train']['total']
            ret[typ] = {
                'other': others_count,
                'usage_rate': usage_rate,
                'total': data['total'],
                'working': data['train']['schedulable']['working'],
                'free': data['train']['schedulable']['free'],
            }
        resp = {
            'success': 1,
            'result': ret['gpu'].copy()
        }
        resp['result']['gpu_detail'] = ret['gpu']
        if not user.is_external:
            resp['result']['cpu_detail'] = ret['cpu']
        return resp
    except Exception as e:
        print(f'Internal calculation error: {str(e)}')
        return {
              'success': 0,
              'msg': 'Internal calculation error'
        }

