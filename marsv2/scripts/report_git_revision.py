
import haienv
haienv.set_env('202111')

import os

from hfai.client.api.training_api import send_data

commit_sha = os.environ.get('MARSV2_CURRENT_GIT_COMMIT_SHA')

try:
    send_data(data={
        'source': 'report_git_revision',
        'rank': os.environ.get('MARSV2_RANK'),
        'commit_sha': commit_sha
    })
except Exception as e:
    print('ERROR: ' + str(e))
    exit(1)
