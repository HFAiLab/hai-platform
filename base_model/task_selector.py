
class TaskSelector:
    @classmethod
    def find_one(cls, impl_cls, **kwargs):
        raise NotImplementedError

    @classmethod
    def find_list(cls, impl_cls, **kwargs):
        raise NotImplementedError

    @classmethod
    def get_error_info(cls, id, *args, **kwargs):
        raise NotImplementedError
