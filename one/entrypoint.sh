#!/bin/bash

set -e

echo "==== label nodes ===="
MARS_GROUP_FLAG=$(python -c "import conf; print(conf.MARS_GROUP_FLAG)")
if [[ "${TRAINING_GROUP}" != "" ]]; then
  echo "kubectl label nodes ${TRAINING_NODES} ${MARS_GROUP_FLAG}=${TRAINING_GROUP}"
  kubectl label nodes ${TRAINING_NODES} ${MARS_GROUP_FLAG}=${TRAINING_GROUP} --overwrite
fi

if [[ "${JUPYTER_GROUP}" != "" ]]; then
  echo "kubectl label nodes ${JUPYTER_NODES} ${MARS_GROUP_FLAG}=${JUPYTER_GROUP}"
  kubectl label nodes ${JUPYTER_NODES} ${MARS_GROUP_FLAG}=${JUPYTER_GROUP} --overwrite
fi

echo "==== start redis ===="
envsubst < one/thirdparty_conf/redis.conf >/etc/redis/redis.conf
service redis-server start

echo "==== start postgres ===="
count=$(ls ${PG_DATA_DIR} |wc -w)
if [[ $count -gt 0 ]]; then
  chown -R postgres:postgres ${PG_DATA_DIR}
  echo "$PG_DATA_DIR already inited"
else
  chown postgres:postgres ${PG_DATA_DIR}
  chmod 750 ${PG_DATA_DIR}
  pushd /tmp
    sudo -u postgres /usr/lib/postgresql/12/bin/initdb -D ${PG_DATA_DIR} -E utf-8
  popd
fi
cp one/thirdparty_conf/postgresql.conf /etc/postgresql/12/main/postgresql.conf
cp one/thirdparty_conf/pg_hba.conf /etc/postgresql/12/main/pg_hba.conf
service postgresql start
pushd /tmp
  # 尝试创建用户
  set +e
  sudo -u postgres createuser -s -i -d -r -l -w ${POSTGRES_USER}
  sudo -u postgres psql -c "ALTER ROLE ${POSTGRES_USER} WITH PASSWORD '${POSTGRES_PASSWORD}';"
  sudo -u postgres psql -c  "CREATE DATABASE mars_db"
  set -e
popd

PGUSER=${POSTGRES_USER} PGPASSWORD=${POSTGRES_PASSWORD} NO_CI_DB_FILE=1 INIT_SQL=/tmp/init.sql bash deploy/dbs/files/init_postgresql.sh

LOG_DIR=/high-flyer/log
mkdir -p ${LOG_DIR}

echo "==== start hai-one using config ${MARSV2_MANAGER_CONFIG_DIR} ===="
# 配置 haproxy
cp one/thirdparty_conf/haproxy.cfg /etc/haproxy/haproxy.cfg

mkdir -p /etc/hai_one_config
cp -r ./one/one_etc/core.toml /etc/hai_one_config/core.toml
cp -r ./one/one_etc/extension.toml /etc/hai_one_config/extension.toml
cp -r ./one/one_etc/scheduler.toml /etc/hai_one_config/scheduler.toml
export MARSV2_MANAGER_CONFIG_DIR=/etc/hai_one_config

# 配置 studio 和 monitor 通用的环境变量
export CLUSTER_SERVER_URL=http://${HAI_SERVER_ADDR}
export BFF_URL=http://${BFF_ADDR}
export WS_URL=ws://${BFF_ADDR}
# 配置 studio 的环境变量
export STUDIO_CLUSTER_REDIS=redis://:${REDIS_PASSWORD}@0.0.0.0:6379/0
export STUDIO_PORT=8080
export STUDIO_CLUSTER_PGSQL=postgres://${POSTGRES_PASSWORD}:${POSTGRES_USER}@0.0.0.0:5432/mars_db
export STUDIO_BFF_REDIS=redis://:${REDIS_PASSWORD}@0.0.0.0:6379/8
export STUDIO_ENABLE_FETION=false
export STUDIO_ENABLE_ONLINE_DEBUG=true
export STUDIO_ENABLE_COUNTLY=false
export STUDIO_JUPYTER_URL=http://${INGRESS_HOST}
export NODEJS_ENV=prod
export PREPUB=is_false

# export DEBUG_PKG=1 可以打印出虚拟文件系统来

if [[ ${MANUAL_START_SEVER} == "1" ]]; then
  echo "please manual start server"
  # start haproxy
  haproxy -f /etc/haproxy/haproxy.cfg 2>&1 | rotatelogs -n 7 ${LOG_DIR}/haproxy.log 86400 &

  start_server() {
  MODULE_NAME=${1} REPLICA_RANK=0 SERVER=${2} python -u uvicorn_server.py --port "${3}" 2>&1 \
    | rotatelogs -n 7 ${LOG_DIR}/"${2}"_0.log 86400 &
  }

  start_worker() {
    MODULE_NAME=${1} REPLICA_RANK=0 LAUNCHER_COUNT=1 python -u "${2}".py ${3} 2>&1 \
      | rotatelogs -n 7 ${LOG_DIR}/"${2}"_0.log 86400 &
  }

  start_worker k8swatcher k8s_watcher
  start_worker launcher launcher
  start_worker scheduler scheduler

  start_server query-server query 8081
  start_server server operating 8082
  start_server ugc-server ugc 8083
  start_server monitor-server monitor 8084
else
    supervisord -c one/supervisord.conf
fi

echo 'waiting 10 seconds for all start...'
sleep 10
tail -f ${LOG_DIR}/*.log
