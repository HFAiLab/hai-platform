
import json

# 下面接口在训练中调用 =====================================
from fastapi import Depends

from conf import CONF
from api.depends import get_api_task
from base_model.training_task import TrainingTask
from conf.flags import STOP_CODE
from db import a_redis as redis
from roman_parliament import register_archive, remove_archive_locally
from server_model.training_task_impl import TaskApiImpl


async def fail_task(rank: int, t: TrainingTask=Depends(get_api_task()), hw_check_pass: int = 1, err_msg: str = ''):
    if err_msg:  # 这肯定是ib或者ecc error传过来的
        # 不发告警，一切都由主动防御来处理
        await redis.lpush('fail_task_node_err_channel', json.dumps({'id': t.id, 'err_msg': err_msg, 'rank': rank}))
    stop_code = 0
    recorded_stop_code = await redis.get(f'lifecycle:{t.id}:stop_code')
    if recorded_stop_code:
        for code in recorded_stop_code.decode().strip().split('\n'):
            stop_code |= int(code)
    if stop_code & STOP_CODE.MANUAL_STOP:
        return {
            'success': 1,
            'msg': '用户调用了 stop 接口，不再处理 hook failed'
        }
    register_archive(t, sign='id')
    t.re_impl(TaskApiImpl).update_pod_status(rank=rank, status='failed')
    remove_archive_locally(t)

    if hw_check_pass:
        await redis.lpush(
            f'{CONF.manager.stop_channel}:{t.id}',
            json.dumps({'action': 'stop', 'flag': STOP_CODE.FAILED})
        )
    else:
        await redis.lpush(
            f'{CONF.manager.stop_channel}:{t.id}',
            json.dumps({'action': 'stop', 'flag': STOP_CODE.HOOK_RESTART})
        )
        # 不cordon，由主动防御来处理
    return {
        'success': 1,
        'msg': f'成功发送将{t.id}改为failed的指令'
    }
