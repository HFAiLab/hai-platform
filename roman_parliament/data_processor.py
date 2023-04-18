from conf.flags import PARLIAMENT_SOURCE_TYPE
from .attr_hooks import get_attr_hook
from .archive import archive_dict
from .archive_triggers import run_archive_create_trigger
from .mass import record_mass, remove_mass


class ModuleProcessor:
    @classmethod
    def update(cls, **data):
        """
        某档案的属性被更新
        :param data['class']: 被更新的类名
        :param data['attr']: 被更新的属性，一定有real_前缀，在找hook时得把这个前缀给删掉
        :param data['validate_attr']: 用于验证的sign名，例如TrainingTask中为'id'
        :param data['validate_value']: 用于验证的sign的值，例如TrainingTask中为'id'的值
        """
        get_attr_hook(class_name=data['class'], attr=data['attr'][len('real_'):]).update_func(data, archive_dict)

    @classmethod
    def create_archive(cls, **data):
        """
        得知有新档案被创建
        :param data['archive']: 被创建的档案实例
        :param data['sign']: 该档案用于验证的唯一属性，例如TrainingTask中为'id'
        """
        run_archive_create_trigger(trigger_name=data['trigger_name'], data=data['data'])

    @classmethod
    def register_mass(cls, **data):
        """
        有群众注册进议会中
        :param data['key_list']: 群众对应的档案key列表
        :param data['mass_name']: 群众名
        """
        try:
            record_mass(key_list=data['key_list'], mass_name=data['mass_name'])
        except:  # 简单兼容下以前的协议
            pass

    @classmethod
    def cancel_mass(cls, **data):
        """
        有群众要退出会议
        :param data['mass_name']: 群众名
        """
        try:
            remove_mass(mass_name=data['mass_name'])
        except:  # 简单兼容下以前的协议
            pass

    @classmethod
    def cancel_archive(cls, **data):
        """
        撤销议员档案袋中的档案
        :param data['key']: 被撤销的档案key
        """
        archive_dict.pop(data['key'], None)


class DataProcessor:
    """
    在收到消息时，进行一系列的处理
    """
    @classmethod
    def run(cls, source, data):
        """
        :param source: str，消息类型，参考PARLIAMENT_SOURCE_TYPE
        :param data: dict，根据不同的source制定相应的key
        :return: List: 需要下放的成员
        """
        return cls.processor_map[source](**data)

    processor_map = {
        PARLIAMENT_SOURCE_TYPE.UPDATE: ModuleProcessor.update,
        PARLIAMENT_SOURCE_TYPE.CREATE_ARCHIVE: ModuleProcessor.create_archive,
        PARLIAMENT_SOURCE_TYPE.REGISTER_MASS: ModuleProcessor.register_mass,
        PARLIAMENT_SOURCE_TYPE.CANCEL_MASS: ModuleProcessor.cancel_mass,
        PARLIAMENT_SOURCE_TYPE.CANCEL_ARCHIVE: ModuleProcessor.cancel_archive
    }


__all__ = ['DataProcessor']
