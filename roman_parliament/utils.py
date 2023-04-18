from conf import CONF
import os


def generate_key(class_name, sign, value):
    """
    通过class_name, sign和value生成档案的唯一key
    :param class_name:
    :param sign:
    :param value:
    :return:
    """
    return f'{class_name}&{sign}&{value}'


def is_senator():
    return os.environ.get('MODULE_NAME', '') in CONF.parliament.senator_list
