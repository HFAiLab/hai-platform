#!/bin/bash
# 检查用户自定义镜像是否满足依赖等限制条件, 脚本可以公开给外部用户供其自行检验
# 只是一些基础的校验, 通过不一定能起的来任务, 主要是为了防止任务起得来但各种 helper 起不来, 导致任务失控的情况
# (比如 jupyter 没了 helper 就不超时等等)

[[ $MARSV2_TASK_BACKEND == train_image:* ]] || exit 0

set -e
echo "检查自定义镜像的基础依赖"

####### Python Environment Check #######

echo "> Checking python3 and pip3"

# python3
python3_path=$(which python3) || { echo "FAILED: python3 not found in PATH"; exit 1; }
pip3_path=$(which pip3) || { echo "FAILED: pip3 not found in PATH"; exit 1; }

echo "- python3: $python3_path"
echo "- pip3: $pip3_path"

# python >= 3.6
python3 -c 'import sys; ver=sys.version_info; assert ver[0]==3 and ver[1] >= 6' >/dev/null 2>&1 \
    || { echo 'FAILED: python<3.6'; exit 1; }

# python requirements
echo ">" Checking python requirements

# Helper Scripts Dependencies
declare -a requirements=(
    "psutil"
    "sysv-ipc"
    "pyzmq"
)

for requirement in "${requirements[@]}"
do
    echo "- $requirement"
    pip3 install "$requirement" --timeout 1e-12 --retries 0 --no-cache-dir > /dev/null || { echo "FAILED: python 缺少依赖 [$requirement]"; exit 1; }
done

echo Passed.
