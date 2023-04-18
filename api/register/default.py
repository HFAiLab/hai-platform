

import os
import functools
import logging

from api.app import app


logger = logging.getLogger("uvicorn")  # stdout req and res
app.post = functools.partial(app.post)
REG_SERVERS = {s for s in os.environ['SERVER'].split(',')}
