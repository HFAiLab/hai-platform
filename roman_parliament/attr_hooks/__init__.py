from .utils import get_attr_hook, set_attr_hook
from . import parliament_attr_hook, pods_hook
from .parliament_attr_hook import generate_parliament_attr_value


def set_attr_hooks():
    from .utils import set_attr_hook

    from base_model.training_task import TrainingTask
    from .parliament_attr_hook import ParliamentAttrHook
    from .pods_hook import PodsHook
    set_attr_hook(f'registered_{TrainingTask.__name__}', 'parliament_attr', ParliamentAttrHook)
    set_attr_hook(f'registered_{TrainingTask.__name__}', '_pods_', PodsHook)
