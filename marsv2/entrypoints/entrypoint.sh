#!/bin/bash
set -e

source /marsv2/scripts/init/hf_envs.values
echo "source /marsv2/scripts/init/hf_envs.values" >> /etc/bash.bashrc

source /marsv2/scripts/init/pod_env.values
echo "source /marsv2/scripts/init/pod_env.values" >> /etc/bash.bashrc

source /marsv2/scripts/init/haiprof_envs.values
echo "source /marsv2/scripts/init/haiprof_envs.values" >> /etc/bash.bashrc

source /marsv2/entrypoints/system_scope.sh

# 记录当前的运行任务
LAST_TASK_DIR=/marsv2/last_task/${MARSV2_TASK_TYPE}
mkdir -p ${LAST_TASK_DIR}
cp /marsv2/scripts/init/task.json ${LAST_TASK_DIR}

if [[ ${MARSV2_ASSIGNED_NUMA} == '0' || ${MARSV2_ASSIGNED_NUMA} == '1' ]]; then
user_cmd=$(cat <<EOF
ulimit -n 204800 \
&& export PYTHONPATH=${PWD}:${PYTHONPATH} \
&& numactl -m ${MARSV2_ASSIGNED_NUMA} -N ${MARSV2_ASSIGNED_NUMA} bash /marsv2/entrypoints/user_scope.sh 2>&1
EOF
)
else
user_cmd=$(cat <<EOF
ulimit -n 204800 \
&& export PYTHONPATH=${PWD}:${PYTHONPATH} \
&& bash /marsv2/entrypoints/user_scope.sh 2>&1
EOF
)
fi

set -o pipefail
if [[ -a /marsv2/scripts/ambient/ambient ]]; then
  /marsv2/scripts/ambient/ambient /sbin/runuser --fast ${MARSV2_USER} --preserve-environment -s /bin/bash -c "${user_cmd}" \
    | LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/marsv2/scripts/fountain /marsv2/scripts/fountain/fountain "cat" "python3 -u /marsv2/scripts/task_log_helper.py" \
    | ts '[%Y-%m-%d %H:%M:%.S]' \
    | pv -L ${MAX_OPS} | rotatelogs -n ${NUMBER_OF_FILES} ${MARSV2_LOG_FILE_PATH} ${MAX_FILESIZE}
  else
  /sbin/runuser --fast ${MARSV2_USER} --preserve-environment -s /bin/bash -c "${user_cmd}" \
    | ts '[%Y-%m-%d %H:%M:%.S]' \
    | pv -L ${MAX_OPS} | rotatelogs -n ${NUMBER_OF_FILES} ${MARSV2_LOG_FILE_PATH} ${MAX_FILESIZE}
fi
set +o pipefail

MARSV2_SYSTEM_SCRIPT_DIR=/usr/local/sbin/hf-scripts
if [[ -d ${MARSV2_SYSTEM_SCRIPT_DIR}/post_user_scope ]]; then
  for script in `ls ${MARSV2_SYSTEM_SCRIPT_DIR}/post_user_scope | sort -n`; do
    source ${MARSV2_SYSTEM_SCRIPT_DIR}/post_user_scope/${script}
  done
fi

# system finish
echo [finish training ${MARSV2_NB_NAME}\(${MARSV2_TASK_ID}\) on ${MARSV2_NODE_NAME} for ${MARSV2_USER}] | ts '[%Y-%m-%d %H:%M:%.S]' > ${MARSV2_LOG_FILE_PATH}.${NUMBER_OF_FILES}
echo [finish training ${MARSV2_NB_NAME}\(${MARSV2_TASK_ID}\) on ${MARSV2_NODE_NAME} for ${MARSV2_USER}] | ts '[%Y-%m-%d %H:%M:%.S]' >> ${MARSV2_DEBUG_LOG_FILE_PATH}
chown -R ${MARSV2_USER}:${MARSV2_USER} ${MARSV2_LOG_FILE_PATH}.${NUMBER_OF_FILES}
# waiting master pods done
# rank0 得等其它rank都结束了才能结束，不然log helper会失效
if [[ ${RANK} == 0 ]]; then
  python3 /marsv2/scripts/waiting_pods_done.py
fi
