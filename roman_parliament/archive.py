from .utils import generate_key
from conf import CONF
from conf.flags import PARLIAMENT_SOURCE_TYPE
import inspect
from .attr_hooks import get_attr_hook
from .backends import backend

archive_dict = {}


class InstanceDescription(object):
    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        return getattr(instance, f'real_{self.name}')

    def __set__(self, instance, value):
        return get_attr_hook(instance.__class__.__name__, self.name).set_func(self, instance, value)

def register_archive(archive, sign, ignore_attr_list=None):
    """
    把 archive 注册进档案袋里
    :param archive: 某个类的实例，作为档案丢入档案袋中
    :param sign: 该档案的唯一标识，在 Task 中为 "id"
    :return: None
    """
    global archive_dict
    attr_list = dir(archive)
    attr_list = list(filter(
    lambda x: not(x.startswith('__') and x.endswith('__'))
              and not(hasattr(archive.__class__, x) and isinstance(getattr(archive.__class__, x), property))    # 不考虑 property
              and not(hasattr(archive.__class__, x) and inspect.isroutine(getattr(archive.__class__, x)))       # 不考虑函数和方法
              and (ignore_attr_list is None or x not in ignore_attr_list),
        attr_list)
    )
    class_attr_dict = {}
    for attr in attr_list:
        value = getattr(archive, attr)
        setattr(archive, f'real_{attr}', value)
        class_attr_dict[attr] = InstanceDescription()
    assert not hasattr(archive, CONF.parliament.validate_attr), \
        f'该实例拥有属性 {CONF.parliament.validate_attr}，可能被重复注册，无法被注册进档案袋中'  # 一般不会遇到，兜个底
    setattr(archive, CONF.parliament.validate_attr, sign)
    new_class = type(f'registered_{archive.__class__.__name__}', (archive.__class__,), class_attr_dict)
    archive.__class__ = new_class
    dict_key = generate_key(archive.__class__.__name__, sign, getattr(archive, sign))
    archive_dict[dict_key] = archive
    return None


def remove_archive_locally(archive):
    global archive_dict
    sign = getattr(archive, CONF.parliament.validate_attr)
    dict_key = generate_key(archive.__class__.__name__, sign, getattr(archive, sign))
    archive_dict.pop(dict_key, None)


def cancel_archive(archive, sign):
    data = {
        'source': PARLIAMENT_SOURCE_TYPE.CANCEL_ARCHIVE,
        'data': {
            'key': generate_key(archive.__class__.__name__, sign, getattr(archive, sign))
        }
    }
    backend.set(data)  # 告知要从档案袋中剔除该档案


def add_archive_for_senators(trigger_name, data):
    """
    把新创建的实例告知所有议员，议员将调用对应trigger来构造新档案
    :param trigger_name: 议员要跑的trigger名
    :param data: 发送的数据
    :return:
    """
    data = {
        'source': PARLIAMENT_SOURCE_TYPE.CREATE_ARCHIVE,
        'data': {
            'trigger_name': trigger_name,
            'data': data
        }
    }
    backend.set(data)
