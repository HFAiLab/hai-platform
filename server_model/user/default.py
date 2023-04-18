
from base_model.base_user import UserModuleDescriptor


class ServerUserModule(UserModuleDescriptor):
    @property
    def module_class(self):
        # 使用到用户组件的时候再 import 进来, 避免循环依赖导致组件的实现无法调用 User 类
        assert isinstance(self.type_annotation, str)    # __future__.annotations 会将标注转换成字符串
        from server_model import user_impl
        return getattr(user_impl, self.type_annotation)


class UserExtras:
    pass
