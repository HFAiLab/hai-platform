

import datetime
from typing import Optional

from server_model.user import User
from server_model.user_data import UserTable, UserAccessTokenTable


class UserSelector:
    @classmethod
    def __df_to_users(cls, df, with_token=True):
        # 不需要 token 时需要指定 access token 形式的 dummy token, 否则会耗时查数据库拿 access token
        overwrite_token = {} if with_token else {'token': 'ACCESS-dummy'}
        return [User(**{**row, **overwrite_token}) for row in df.to_dict('records')]

    @ classmethod
    def from_token(cls, token: str, allow_expired=False) -> Optional[User]:
        # access token 的方式，查用户再更新准入范围
        if token.startswith('ACCESS-'):
            access_token = token
            df = UserAccessTokenTable.df
            __filter = (df.access_token == access_token)
            if not allow_expired:
                __filter = __filter & (df.expire_at > datetime.datetime.now()) & df.active
            user_access = df[__filter]
            if len(user_access) == 1:
                user_access = user_access.iloc[0]
                user = cls.from_user_name(user_name=user_access.access_user_name)
                if user is not None:
                    user.access.access_scope = user_access.access_scope
                    user.access.from_user_name = user_access.from_user_name
                    user.access.expire_at = user_access.expire_at
                    user.token = access_token
                    return user
            return None
        # 原初 token 的方式
        else:
            df = UserTable.df
            df = df[df.token == token]
            if len(df) > 0:
                return cls.__df_to_users(df[:1])[0]
            else:
                return None

    @classmethod
    def from_user_name(cls, user_name):
        df = UserTable.df
        df = df[df.user_name == user_name]
        if len(df) > 0:
            return cls.__df_to_users(df[:1])[0]
        else:
            return None

    @classmethod
    def fetch_all(cls, with_token=False):
        return cls.__df_to_users(UserTable.df, with_token=with_token)
