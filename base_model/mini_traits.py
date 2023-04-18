# 参考了 ipython 的 traitlets, 做了精简，只提供了方便我们 validation 和 初始化的功能
from datetime import datetime
from dateutil.parser import parse


class MiniType:
    def __init__(self, default_value=None):
        self.default_value = self.validate(default_value)

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance._trait_values.get(self.name, self.default_value)

    def __set__(self, instance, value):
        instance._trait_values[self.name] = self.validate(value)

    def instance_init(self, instance, value):
        instance._trait_values[self.name] = self.validate(value) if value else self.default_value

    def validate(self, value):
        return value


class MiniTraits:
    def __init__(self, **kwargs):
        self._trait_values = {}

        cls = self.__class__
        for key in dir(cls):
            try:
                value = getattr(cls, key)
            except AttributeError:
                pass
            else:
                if isinstance(value, MiniType):
                    value.instance_init(self, kwargs.get(key, None))

    def trait_dict(self):
        return self._trait_values

    def remove_trait(self, name):
        """
        删除其中的一个值， 这是永久生效的
        :param name:
        :return:
        """
        del self._trait_values[name]


class Datetime(MiniType):
    def __init__(self, default_value=datetime.fromtimestamp(86400)):
        super().__init__(default_value)

    def validate(self, value):
        if not isinstance(value, datetime):
            if isinstance(value, str):
                value = parse(value)
            else:
                value = datetime.fromtimestamp(86400)
        return value


Any = MiniType
NoneInt = MiniType
NoneStr = MiniType


class Int(MiniType):
    def __init__(self, default_value=0):
        super().__init__(default_value)


class Str(MiniType):
    def __init__(self, default_value=''):
        super().__init__(default_value)


Unicode = Str


class List(MiniType):
    def __init__(self, default_value=[]):
        super().__init__(default_value)


class Dict(MiniType):
    def __init__(self, default_value={}):
        super().__init__(default_value)


class Bool(MiniType):
    def __init__(self, default_value=False):
        super().__init__(default_value)
