

import datetime
from typing import Optional, List

from server_model.user import User
from server_model.user_data import UserTable, UserAccessTokenTable


class AioUserSelector:
    @classmethod
    def __df_to_users(cls, df, with_token=True):
        # 不需要 token 时需要指定 access token 形式的 dummy token, 否则会耗时查数据库拿 access token
        overwrite_token = {} if with_token else {'token': 'ACCESS-dummy'}
        return [User(**{**row, **overwrite_token}) for row in df.to_dict('records')]

    @classmethod
    async def find_all(cls, max_num_limit=None, with_token=False, **kwargs) -> List[User]:
        f"""
            with_token: 是否需要获取 token, 如果需要的话需对每个用户查数据库拿 access token, 速度较慢
            支持筛选的 columns: check {UserTable.columns}
        """
        df = await UserTable.async_df
        assert all(field in df.columns for field in kwargs.keys()), \
            f'查找用户的筛选条件不存在: {[k for k in kwargs if k not in df.columns]}'

        for field, value in kwargs.items():
            df = df[df.get(field) == value]
        df = df if max_num_limit is None else df[:max_num_limit]
        return cls.__df_to_users(df, with_token=with_token)

    @classmethod
    async def find_one(cls, **kwargs) -> Optional[User]:
        if kwargs.get('token') is not None:
            return await cls.from_token(kwargs['token'])
        users = await cls.find_all(max_num_limit=1, with_token=True, **kwargs)
        if len(users) > 0:
            return users[0]
        else:
            return None

    @ classmethod
    async def from_token(cls, token: str, allow_expired=False) -> Optional[User]:
        # access token 的方式，查用户再更新准入范围
        if token.startswith('ACCESS-'):
            access_token = token
            df = await UserAccessTokenTable.async_df
            __filter = (df.access_token == access_token)
            if not allow_expired:
                __filter = __filter & (df.expire_at > datetime.datetime.now()) & df.active
            user_access = df[__filter]
            if len(user_access) == 1:
                user_access = user_access.iloc[0]
                users = await cls.find_all(max_num_limit=1, user_name=user_access.access_user_name)
                if len(users) > 0:
                    user = users[0]
                    user.access.access_scope = user_access.access_scope
                    user.access.from_user_name = user_access.from_user_name
                    user.access.expire_at = user_access.expire_at
                    user.token = access_token
                    return user
            return None
        # 原初 token 的方式
        else:
            users = await cls.find_all(max_num_limit=1, with_token=True, token=token)
            if len(users) > 0:
                return users[0]
            else:
                return None
