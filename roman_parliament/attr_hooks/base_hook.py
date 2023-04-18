from conf import CONF
from conf.flags import PARLIAMENT_SOURCE_TYPE
from roman_parliament.utils import generate_key
from roman_parliament.backends import backend


class BaseHook:
    @classmethod
    def __base_data(cls, description, instance, value):
        return {
            'source': PARLIAMENT_SOURCE_TYPE.UPDATE,   # 更新标志，不能换
            'data': {
                'class': instance.__class__.__name__,  # 用于识别func，不能换
                'validate_attr': getattr(instance, CONF.parliament.validate_attr),  # 用于验证是否是需要被更新的档案
                'validate_value': getattr(instance, f'real_{getattr(instance, CONF.parliament.validate_attr)}'),
                'attr': f'real_{description.name}',  # 用于识别func，不能换
                'value': value
            }
        }

    @classmethod
    def get_data(cls, description, instance, value, base_data):
        """
        含泪劝告，不要动base_data中已有的属性，具体可以见__base_data
        """
        return base_data

    @classmethod
    def set_attr(cls, description, instance, value):
        setattr(instance, f'real_{description.name}', value)

    @classmethod
    def broadcast_data(cls, data, setting_result):
        backend.set(data, mass=True)

    @classmethod
    def set_func(cls, description, instance, value):  # 可以自己设置传过去的数据，传的方式，以及做的操作
        """
        含泪劝告，不要覆盖 set_func 方法，可以覆盖get_data, broadcast_data和set_attr方法
        当一个被注册的实例修改其属性时，会走set_func，首先生成要传播的数据，然后进行传播，最后设置该实例本身的属性
        """
        data = cls.get_data(description, instance, value, cls.__base_data(description, instance, value))
        setting_result = cls.set_attr(description, instance, value)  # 可能要做写数据库的操作，先写再发
        cls.broadcast_data(data, setting_result)

    @classmethod
    def update_attr(cls, archive, data):
        setattr(archive, data['attr'], data['value'])

    @classmethod
    def update_func(cls, data, archive_dict):
        """
        含泪劝告，不要覆盖update_func方法，可以覆盖update_attr
        当收到其它成员传过来的消息时，找到档案袋中对应的档案，并对其进行同步
        """
        dict_key = generate_key(data['class'], data['validate_attr'], data['validate_value'])
        if dict_key in archive_dict:
            cls.update_attr(archive_dict[dict_key], data)
            return
