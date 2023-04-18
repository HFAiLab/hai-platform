
# 为了安全期间，external 的操作和 internal admin 对 quota 的操作，我拆分成了两个接口
from fastapi import HTTPException
from conf.flags import TASK_PRIORITY
from server_model.user import User


def check_permission(user: User, granted_group):
    if not user.in_group(granted_group):
        raise HTTPException(status_code=403, detail={
            'success': 0,
            'msg': '无权做此操作'
        })


def get_priority_str(priority: int):
    priority_strs = [i[0] for i in TASK_PRIORITY.items() if i[1] == priority]
    if len(priority_strs) == 0:
        raise HTTPException(status_code=403, detail={
            'success': 0,
            'msg': '不存在的优先级'
        })
    priority_str = priority_strs[0]
    return priority_str


def get_all_priority_str():
    return [i[0] for i in TASK_PRIORITY.items() if i[1] >= 0]
