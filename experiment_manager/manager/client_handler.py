import os
import time
from threading import Lock

import ujson

from base_model.training_task import TrainingTask
from conf import CONF
from conf.flags import SUSPEND_CODE, STOP_CODE, TASK_PRIORITY
from db import redis_conn
from experiment_manager.manager.manager_utils import get_log_uuid
from logm import logger, bind_logger_task
from roman_parliament import register_archive, set_mass_info, register_parliament
from roman_parliament.utils import generate_key
from server_model.auto_task_impl import AutoTaskSchemaWithDbImpl
from server_model.selector import TrainingTaskSelector
from server_model.selector import UserSelector
from server_model.task_runtime_config import TaskRuntimeConfig
from server_model.user_data import initialize_user_data_roaming


module = os.path.basename(__file__)
log_id = get_log_uuid(module)
with logger.contextualize(uuid=f'{log_id}.init'):
    task_id = int(os.environ['TASK_ID'])
    initialize_user_data_roaming(overwrite_enable_roaming=False)
    set_mass_info(key_list=[generate_key(class_name=TrainingTask.__name__, sign='id', value=task_id)], mass_name=f'{task_id}_{module}')
    register_parliament()
    task = TrainingTaskSelector.find_one_by_id(AutoTaskSchemaWithDbImpl, id=int(os.environ['TASK_ID']))
    bind_logger_task(task)
    register_archive(task, sign='id')
    user = UserSelector.from_user_name(task.user_name)


def set_whole_life_state(whole_life_state, **kwargs):
    task.update(('whole_life_state',), (whole_life_state,))
    return {
        'success': 1,
        'msg': f'训练任务[{task.job_info}] 标记任务生命周期 {whole_life_state} 成功'
    }


def receive_suspend_command(**kwargs):
    task.update(('suspend_code',), (SUSPEND_CODE.SUSPEND_RECEIVED,))
    return {
        'success': 1,
        'msg': f'训练任务[{task.job_info}] 收到了打断命令，设置 {SUSPEND_CODE.SUSPEND_RECEIVED} 成功'
    }


def go_suspend(**kwargs):
    task.update(('suspend_code',), (SUSPEND_CODE.CAN_SUSPEND,))
    redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}',
                      ujson.dumps({'action': 'stop', 'flag': STOP_CODE.INTERRUPT}))
    return {
        'success': 1,
        'msg': f'训练任务[{task.job_info}]收到了指令，可以被打断'
    }


def set_priority(priority: int = None, custom_rank: float = None, **kwargs):
    if priority:
        if user.is_external:
            priority = -1
        try:
            priority = int(priority)
            if priority not in TASK_PRIORITY.internal_priorities(with_auto=True):
                raise Exception()
        except:
            return {
                'success': 0,
                'msg': '优先级设置不对，请参考 hfai.client.EXP_PRIORITY'
            }
    if priority is not None:
        task.update(('priority', ), (priority, ))
    runtime_config_json = {
        'update_priority_called': True
    }
    if custom_rank is not None:
        runtime_config_json['custom_rank'] = custom_rank
    TaskRuntimeConfig(task).insert('runtime_priority', runtime_config_json, chain=True, update=True)
    return {
        'success': 1,
        'msg': '优先级设置成功'
    }


def disable_warn(warn_type, **kwargs):
    redis_conn.set(f'disable_warn:{task.id}', f'{warn_type}')
    return {
        'success': 1,
        'msg': '设置静默告警成功'
    }


def waiting_memory_free_failed(error_msg, node, **kwargs):
    msg = f'[{task.user_name}][{task.nb_name}][{task.id}] 训练前检查资源释放失败（{error_msg}），请系统组检查'
    logger.f_error(msg)
    task.update(('suspend_code',), (SUSPEND_CODE.CAN_SUSPEND,))
    redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}', ujson.dumps({'action': 'stop', 'flag': STOP_CODE.INTERRUPT}))
    redis_conn.lpush(CONF.manager.node_memory_leak_channel, ujson.dumps({'node': node, 'msg': msg, 'time': int(time.time())}))
    redis_conn.expire(CONF.manager.node_memory_leak_channel, 3600)
    return {
        'success': 1,
        'msg': '发送报警成功'
    }


git_rev_lock = Lock()

def report_git_revision(rank, commit_sha, **kwargs):
    with git_rev_lock:
        if task.config_json.get('git_commit_sha', '') == '':
            task.update(fields=('config_json', ), values=({'git_commit_sha': commit_sha}, ))
            logger.info(f'rank {rank} reported first git revision {commit_sha}')
        elif task.config_json['git_commit_sha'] != commit_sha:
            # 有 rank 报告了不一致的 commit sha, 可能是在起 pod 期间远端 branch/tag 的 HEAD 变化了, 打断任务重启, 重新拉最新的 repo
            task.update(fields=('config_json', ), values=({'git_commit_sha': ''}, ))
            task.update(('suspend_code',), (SUSPEND_CODE.CAN_SUSPEND,))
            redis_conn.lpush(f'{CONF.manager.stop_channel}:{task.id}', ujson.dumps({'action': 'stop', 'flag': STOP_CODE.INTERRUPT}))
            return {'success': 0, 'msg': f'rank {rank} 报告了不一致的 git revision ({commit_sha}), 可能是远端 repo 有更新, 打断任务'}
    return {'success': 1, 'msg': '成功'}
