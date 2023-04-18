from .archive import register_archive, archive_dict, remove_archive_locally, cancel_archive, add_archive_for_senators
from .archive_triggers import add_archive_trigger
from .attr_hooks import set_attr_hooks
from .monitor import register_parliament, withdraw_parliament, has_registered
from .mass import set_mass_info

__all__ = [
    'register_parliament',  # 将议员或者群众注册进会议中
    'has_registered',   # 当前进程是否注册进了会议中
    'archive_dict',  # 档案袋
    'register_archive',  # 注册档案
    'cancel_archive',  # 告知所有议员从档案袋中剔除档案
    'add_archive_for_senators',  # 新注册实例时告知所有议员，可能会被加入进档案袋中
    'add_archive_trigger',  # 添加 trigger
    'remove_archive_locally',  # 在档案袋中删除该档案
    'set_mass_info',  # 设置群众信息
    'withdraw_parliament'  # 退出议会，只有群众才会走这个接口
]
