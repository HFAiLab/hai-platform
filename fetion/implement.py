from .default import *
from .custom import *

from conf import CONF


__all__ = ['Fetion']


class BaseFetion:
    @classmethod
    def alert(cls, msg, **kwargs):
        print(msg)


NameToClass.update({'base_fetion': BaseFetion})

try:
    Fetion = NameToClass[CONF.try_get('fetion.fetion', default='base_fetion')]
except KeyError:
    print(f"fetion配置有问题，未找到{CONF.fetion.get('fetion', 'base_fetion')}，默认不走fetion")
    Fetion = BaseFetion
