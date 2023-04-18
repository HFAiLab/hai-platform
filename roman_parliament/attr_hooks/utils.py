from collections import defaultdict
from .base_hook import BaseHook


attr_hook_dict = defaultdict(lambda: defaultdict(default_class))


def default_class():
    return BaseHook


def set_attr_hook(class_name, attr, hook_class):
    """
    :param class_name: 类名
    :param attr: 属性
    :param hook_class: hook的类
    :return:
    """
    attr_hook_dict[class_name][attr] = hook_class


def get_attr_hook(class_name, attr):
    return attr_hook_dict[class_name][attr]
