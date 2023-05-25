
import ujson

from fastapi import HTTPException, Request

from conf.flags import USER_ROLE
from server_model.selector import AioUserSelector


def get_api_user_with_token(allowed_groups=[], allowed_scopes=[], **kwargs):
    async def __func(request: Request):
        token = None
        if request.url.query:
            req_query = {q.split('=')[0]: q.split('=')[1] for q in request.url.query.split('&') if '=' in q}
            token = req_query.get('token')
        if token is None:
            try:
                token = ujson.loads(await request.body())['token']
            except Exception:
                pass
        if token is None:
                raise HTTPException(status_code=403, detail={
                    'success': 0,
                    'msg': '需要指定 token'
                })
        user = await AioUserSelector.from_token(token=token)
        if user is None:
            expired_user = await AioUserSelector.from_token(token=token, allow_expired=True)
            if expired_user is not None:
                raise HTTPException(status_code=401, detail={
                    'success': 0,
                    'msg': '该 token 已经过期了，请重新提供凭证'
                })
            raise HTTPException(status_code=403, detail={
                'success': 0,
                'msg': '根据 token 未找到用户'
            })
        if not user.active:
            raise HTTPException(status_code=401, detail={
                'success': 0,
                'msg': '您的账号为不活跃状态，无法访问集群服务'
            })
        if len(allowed_groups) > 0 and not user.in_any_group(allowed_groups):
            raise HTTPException(status_code=401, detail={
                'success': 0,
                'msg': '您所在的权限组不允许使用该接口'
            })
        if len(allowed_scopes) > 0 and user.access.access_scope not in allowed_scopes:
            raise HTTPException(status_code=401, detail={
                'success': 0,
                'msg': f'您当前的 access_scope({user.access.access_scope}) 不允许使用该接口'
            })
        for attr, value in kwargs.items():
            try:
                assert getattr(user, attr) == value
            except Exception:
                raise HTTPException(status_code=401, detail={
                    'success': 0,
                    'msg': '您无权访问该接口'
                })
        return user
    return __func


def get_non_external_api_user_with_token(allowed_groups=[], allowed_scopes=[], **kwargs):
    return get_api_user_with_token(allowed_groups=[USER_ROLE.INTERNAL] + allowed_groups, allowed_scopes=allowed_scopes, **kwargs)

