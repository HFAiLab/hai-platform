class BaseBackend:
    @classmethod
    def watch(cls):
        raise NotImplementedError

    @classmethod
    def set(cls, info, mass=False):
        raise NotImplementedError
