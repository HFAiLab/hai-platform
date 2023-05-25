
from conf.flags import USER_ROLE
from server_model.user import User

async def handle_user_usage_exceed():
    return {
        'success': 1,
        'msg': 'not implemented'
    }


def verify_quota_permission(role: str, user: User):
    if user.in_group('internal_quota_limit_editor'):
        return True
    elif role == USER_ROLE.EXTERNAL:
        return user.in_group('external_quota_editor')
    return False
