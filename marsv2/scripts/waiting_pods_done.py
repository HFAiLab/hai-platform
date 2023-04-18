import os
import time
pwd = os.path.dirname(os.environ['MARSV2_DEBUG_LOG_FILE_PATH'])
num = int(os.environ.get('WORLD_SIZE', '0'))

while True:
    n_pretrain = 0
    debug_file_list = [file for file in os.listdir(pwd) if file.startswith('debug')]
    for file in debug_file_list:
        with open(os.path.join(pwd, file), 'r') as f:
            n_pretrain += int('finish training' in f.read())
    if n_pretrain >= num:
        break
    time.sleep(1)
