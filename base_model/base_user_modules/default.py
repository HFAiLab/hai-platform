
class IUserModule:
    def __init__(self, user):
        self.user = user

    def get(self):
        """
        返回用户与此组件相关的常用信息, 对应 HTTP API 的 GET 接口.
        """
        raise NotImplementedError

    async def async_get(self):
        return self.get()


class IUserStorage(IUserModule):
    pass


class IUserImage(IUserModule):
    async def async_get(self):
        raise NotImplementedError

