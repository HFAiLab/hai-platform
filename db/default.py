

import os


if os.environ.get('FAKE_DB', '0') == '1':
    MarsDB = None
    redis_conn, a_redis, redis_config = [None] * 3
else:
    from .mars_db import MarsDB
    from .redis import redis_conn, a_redis, redis_config
