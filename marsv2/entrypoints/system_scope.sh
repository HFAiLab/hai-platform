#!/bin/bash
MARSV2_SYSTEM_SCRIPT_DIR=/usr/local/sbin/hf-scripts

mkdir -p ${MARSV2_SYSTEM_SCRIPT_DIR}/pre_system_init
for script in `ls ${MARSV2_SYSTEM_SCRIPT_DIR}/pre_system_init | sort -n`; do
  source ${MARSV2_SYSTEM_SCRIPT_DIR}/pre_system_init/${script};
done

# create marsV2 dir
mkdir -p ${MARSV2_LOG_DIR}
touch ${MARSV2_LOG_FILE_PATH}
touch ${MARSV2_DEBUG_LOG_FILE_PATH}

# validate custom image
set -o pipefail
/bin/bash /marsv2/scripts/validate_image.sh 2>&1 | ts '[%Y-%m-%d %H:%M:%.S]' >> ${MARSV2_LOG_FILE_PATH}
set +o pipefail

# start hfai scheduler
python3 /marsv2/scripts/suspend_helper.py &
python3 /marsv2/scripts/stop_helper.py &

# unset for running no error
unset NVIDIA_VISIBLE_DEVICES

# create user
if id "${MARSV2_USER}" &>/dev/null; then
    echo "user ${MARSV2_USER} found"
else
    echo "add ${MARSV2_USER}"
    useradd --uid ${MARSV2_UID} -m --home-dir ${HOME} ${MARSV2_USER}
    groupmod -g ${MARSV2_UID} ${MARSV2_USER}
fi
chown ${MARSV2_USER}:${MARSV2_USER} ${HOME}

# manager log watch time
mkdir -p /var/log/mars_hf_log_time
touch /var/log/mars_hf_log_time/${MARSV2_TASK_ID}
chown ${MARSV2_USER}:${MARSV2_USER} /var/log/mars_hf_log_time/${MARSV2_TASK_ID}

if [ "${MARSV2_TASK_TYPE}" != "jupyter" ]; then
  if [[ "${MARSV2_USER}" != "image_loader" && "${MARSV2_USER}" != "inner_dataset_syncer" ]]; then
    # hw check
    if [[ -a /marsv2/scripts/hw-tests/mem_bw/mbw ]]; then
      echo [starting hw check] | ts '[%Y-%m-%d %H:%M:%.S]' >> ${MARSV2_DEBUG_LOG_FILE_PATH}
      OMP_NUM_THREADS=16 OMP_PROC_BIND=SPREAD /marsv2/scripts/hw-tests/mem_bw/mbw | ts '[%Y-%m-%d %H:%M:%.S]' > /var/log/hfai_hw_tests/mbw.log
    fi

    # waiting for master
    echo [waiting master server ${MASTER_ADDR}:${MASTER_PORT}/ip] | ts '[%Y-%m-%d %H:%M:%.S]' >> ${MARSV2_DEBUG_LOG_FILE_PATH}
    source /marsv2/scripts/waiting_for_master.sh
  fi
fi

# for MIG jupyter
if [[ "${MARSV2_NB_GROUP}" == "jd_shared_mig"* ]]; then
  set +e
  export CUDA_VISIBLE_DEVICES=`nvidia-smi -L | grep MIG | awk '{print $6}' | awk -F ')' '{print $1}'`
  set -e
fi

# 给用户授权
set +e
echo "grant user group"
bash /marsv2/scripts/init/grant_user_group.sh
set -e

mkdir -p ${MARSV2_SYSTEM_SCRIPT_DIR}/post_system_init
for script in `ls ${MARSV2_SYSTEM_SCRIPT_DIR}/post_system_init | sort -n`; do
  source ${MARSV2_SYSTEM_SCRIPT_DIR}/post_system_init/${script};
done

# for user log
chown -R ${MARSV2_USER}:${MARSV2_USER} ${MARSV2_LOG_DIR}
