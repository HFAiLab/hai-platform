import os
from logm import logger

def run_audit():
    if os.environ.get('RUN_AUDIT',
                      'false').lower() != 'true' or os.environ.get(
                          'POD_NAME', 'POD_NAME').split('-')[-1] != '0':
        logger.info('audit disabled')
        return
    logger.info('audit not implemented, skip')
    return
