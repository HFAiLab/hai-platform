#!/bin/bash
echo 'this is user scope shell' >> ${MARSV2_DEBUG_LOG_FILE_PATH}
echo [start training ${MARSV2_NB_NAME}\(${MARSV2_TASK_ID}\) on ${MARSV2_NODE_NAME} for ${MARSV2_USER}] | ts '[%Y-%m-%d %H:%M:%.S]' >> ${MARSV2_LOG_FILE_PATH}

set +e

MARSV2_USER_SCRIPT_DIR=/usr/local/bin/hf-scripts

if [[ -d ${MARSV2_USER_SCRIPT_DIR}/pre_user_run ]]; then
  for script in `ls ${MARSV2_USER_SCRIPT_DIR}/pre_user_run | sort -n`; do
    source ${MARSV2_USER_SCRIPT_DIR}/pre_user_run/${script}
  done
fi


#  above is user defined script --------------------------------------------
echo "hfai init" >> ${MARSV2_DEBUG_LOG_FILE_PATH}
pushd ${HOME} > /dev/null
    mkdir -p ${HOME}/.hfai
    echo "url: ${MARSV2_SERVER}" > ${HOME}/.hfai/conf.yml
    echo "token: ${MARSV2_USER_TOKEN}" >> ${HOME}/.hfai/conf.yml
    echo "bff_url: ${MARSV2_BFF_URL}" >> ${HOME}/.hfai/conf.yml
popd > /dev/null
chmod 700 ${HOME}/.hfai

echo "snapshot user scripts" >> ${MARSV2_DEBUG_LOG_FILE_PATH}
if [[ ${RANK} == "0" ]]; then
  mkdir -m u=rwx,go="" -p /marsv2/log/${MARSV2_TASK_ID}/user_scripts
  cp -r /marsv2/scripts/init /marsv2/log/${MARSV2_TASK_ID}/user_scripts &
  # jupyter task 没有 code_file，不需要拷贝
  if [ "${MARSV2_TASK_TYPE}" != "jupyter" ]; then
    if [ "${MARSV2_TASK_ENTRYPOINT_EXECUTABLE}" == "0" ]; then
      cp -r ${MARSV2_TASK_ENTRYPOINT} /marsv2/log/${MARSV2_TASK_ID}/user_scripts &
    fi
  fi
fi

set -e

# waiting memory free
if [ "${MARSV2_TASK_TYPE}" != "jupyter" ]; then
  if [ -a /marsv2/scripts/waiting_memory_free.py ]; then
    python3 -u /marsv2/scripts/waiting_memory_free.py
  fi
fi

# 根据用户提交的任务生成的脚本
bash /marsv2/scripts/init/task_run.sh

if [[ -d ${MARSV2_USER_SCRIPT_DIR}/post_user_run ]]; then
  for script in `ls ${MARSV2_USER_SCRIPT_DIR}/post_user_run | sort -n`; do
    source ${MARSV2_USER_SCRIPT_DIR}/post_user_run/${script}
  done
fi
