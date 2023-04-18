
class UserModuleDescriptor:
    def __set_name__(self, owner, name):
        self.private_name = '_' + name
        assert name in owner.__annotations__, '用户组件必须提供 type annotation'
        self.type_annotation = owner.__annotations__.get(name)

    @property
    def module_class(self):
        assert not isinstance(self.type_annotation, str), 'annotation 必须为实际类型'
        return self.type_annotation

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        if not hasattr(obj, self.private_name):
            setattr(obj, self.private_name, self.module_class(user=obj))
        return getattr(obj, self.private_name)


class BaseUserExtras:
    pass
