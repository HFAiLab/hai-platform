import os
import sys
import uvicorn
from conf import CONF

doc = """hfailab server
Usage:
    uvicorn_server.py [--port=<port>]

Options:
    -p --port=<port>      服务器端口.
"""

from docopt import docopt
arguments = docopt(doc)


if __name__ == "__main__":
    port = int(arguments['--port'])
    server_name = os.environ['SERVER']
    print(f'server [{server_name}] started at: {port}, python', sys.version)
    if server_name == 'log-forest':
        server_module = 'log_forest_server:app'
    elif server_name == 'cloud-storage':
        server_module = 'cloud_storage:app'
    else:  # query monitor ugc operating
        server_module = 'api.register:app'
    uvicorn.run(server_module, host="0.0.0.0", port=port, log_level='debug', access_log=False, workers=CONF.server_workers.get(server_name, 1))
