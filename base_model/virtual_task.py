from .training_task import TrainingTask


class VirtualTask(TrainingTask):  # 目前与TrainingTask无差别
    """
    虚拟任务
    """

    def __init__(self, implement_cls=None, **kwargs):
        super().__init__(implement_cls, **kwargs)
