from .redis import RedisBackend
from conf import CONF

backend_map = {
    'redis': RedisBackend
}

backend = backend_map[CONF.parliament.backend]
__all__ = ['backend']
