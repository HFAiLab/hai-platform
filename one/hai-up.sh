#!/bin/bash

set -e
set -a

set_env() {
  # task namespace
  : ${TASK_NAMESPACE:="hai-platform"}
  # platform namespace, defaults to TASK_NAMESPACE
  : ${PLATFORM_NAMESPACE:=${TASK_NAMESPACE}}
  # shared filesystem root path
  : ${SHARED_FS_ROOT:="/nfs-shared"}
  # kubeconfig file path
  : ${KUBECONFIG:="$HOME/.kube/config"}
  # server address
  : ${HAI_SERVER_ADDR:="47.98.195.232"}
  HAI_SERVER_ADDR=`echo ${HAI_SERVER_ADDR} | sed -e 's/^http:\/\///' -e 's/^https:\/\///'`
  # gpus per node
  : ${NODE_GPUS:=4}
  # ib per node
  : ${HAS_RDMA_HCA_RESOURCE:=0}

  : ${MARS_PREFIX:="hai"}
  # training compute nodes label will be set to ${MARS_PREFIX}_mars_group=${TRAINING_GROUP}
  : ${TRAINING_GROUP:="training"}
  # jupyter compute nodes label will be set to ${MARS_PREFIX}_mars_group=${JUPYTER_GROUP}
  : ${JUPYTER_GROUP:="jupyter_cpu"}
  # training compute nodes list, format: "node1 node2"
  : ${TRAINING_NODES:="cn-hangzhou.172.23.183.227"}
  # jupyter compute nodes list, format: "node1 node2", JUPYTER_NODES should differ from TRAINING_NODES
  : ${JUPYTER_NODES:="cn-hangzhou.172.23.183.226"}
  # service nodes which running task manager, format: "node1 node2"
  : ${MANAGER_NODES:="cn-hangzhou.172.23.183.226"}

  # all in one image
  : ${BASE_IMAGE:="registry.cn-hangzhou.aliyuncs.com/hfai/hai-platform:latest"}
  # train image
  : ${TRAIN_IMAGE:="registry.cn-hangzhou.aliyuncs.com/hfai/hai-platform:latest"}
  # ingress hostname serving studio, jupyter
  : ${INGRESS_HOST:="nginx-ingress-lb.kube-system.c2c348f48c063452fa5738ec9caeb69ea.cn-hangzhou.alicontainer.com"}
  INGRESS_HOST=`echo ${INGRESS_HOST} | sed -e 's/^http:\/\///' -e 's/^https:\/\///'`
  # ingress class
  : ${INGRESS_CLASS:="nginx"}

  # platform user info, format: "user1:uid1:passwd,user2:uid2:passwd"
  : ${USER_INFO:="haiadmin:10020:xxxxxxxx"}
  # postgres password
  : ${POSTGRES_USER:="root"}
  : ${POSTGRES_PASSWORD:="root"}
  # redis password
  : ${REDIS_PASSWORD:="root"}
  # uid of the reserved admin user for bff
  : ${BFF_ADMIN_UID:=10000}
  # bff admin user token
  : ${BFF_ADMIN_TOKEN:=$(echo $RANDOM | md5sum | head -c 20)}

  # postgres database path, defaults to ${SHARED_FS_ROOT}/hai-platform/db
  : ${DB_PATH:=${SHARED_FS_ROOT}/hai-platform/db}

  # add extra mounts to hai-platform, format: "src1:dst1:file:ro,src2:dst2:directory"
  : ${EXTRA_MOUNTS:=""}
  # : ${EXTRA_MOUNTS:="
  # /tmp/server.py:/high-flyer/code/multi_gpu_runner_server/server.py:file,
  # /tmp/postgresql.conf:/etc/postgresql/12/main/postgresql.conf:file"}
  EXTRA_MOUNTS=`echo "${EXTRA_MOUNTS}" | tr -d "\n" | tr -d " "`

  # add extra environments to hai-platform, format: "k1:v1,k2:v2"
  : ${EXTRA_ENVIRONMENTS:=""}
  EXTRA_ENVIRONMENTS=`echo "${EXTRA_ENVIRONMENTS}" | tr -d "\n" | tr -d " "`

  # only generate manifest
  : ${DRY_RUN:=""}

  : ${PROVIDER:="k8s"}

  USER_NAMES=()
  USER_IDS=()
  USER_TOKENS=()
  HAI_PLATFORM_PATH=${SHARED_FS_ROOT}/hai-platform
}

print_step() {
  echo -e "\033[34m${1} \033[0m"
}

print_usage() {
  echo -e "Usage:
  $(basename $0) config/run/up/dryrun/down/upgrade [option]
  where:
    config:  print config script
    run/up:  run hai platform
    dryrun:  generate config template
    down:    tear down hai platform
    upgrade: self upgrade hai-cli/hai-up utility

    option:
      -h/--help:      show this help text
      -p/--provider:  k8s/docker-compose, default to k8s
      -c/--config:    show config scripts to setup environment variables,
                      if not specified, current shell environment will be used,
                      if not shell environment exists, default value in 'hai-up config' will be used
"

  print_step "Setup guide"
  print_step "  step 1: ensure the following dependencies satisfied"
  echo -e "    - a kubernetes cluster with loadbalancer and ingress supported
    - a shared filesystem mounted in current host and other compute nodes
    - for provider docker-compose: docker and docker-compose should be installed in current host"
  print_step '  step 2: "hai-up config > config.sh", modify environment variables in config.sh'
  print_step '  step 3: "hai-up run -c config.sh" to start the all-in-one hai-platform.'
}

print_config_script() {
  echo -e '    export TASK_NAMESPACE="hai-platform" # task namespace
    export SHARED_FS_ROOT="/nfs-shared" # shared filesystem root path
    export MARS_PREFIX="hai"
    export TRAINING_GROUP="training" # training compute nodes label will be set to ${MARS_PREFIX}_mars_group=${TRAINING_GROUP}
    export JUPYTER_GROUP="jupyter_cpu" # jupyter compute nodes label will be set to ${MARS_PREFIX}_mars_group=${JUPYTER_GROUP}
    export TRAINING_NODES="cn-hangzhou.172.23.183.227" # training compute nodes list, format: "node1 node2"
    export JUPYTER_NODES="cn-hangzhou.172.23.183.226" # jupyter compute nodes list, format: "node1 node2", JUPYTER_NODES should differ from TRAINING_NODES
    export MANAGER_NODES="cn-hangzhou.172.23.183.226" # service nodes which running task manager, format: "node1 node2"
    export INGRESS_HOST="nginx-ingress-lb.kube-system.c2c348f48c063452fa5738ec9caeb69ea.cn-hangzhou.alicontainer.com" # ingress hostname serving studio, jupyter，no http prefix needed
    export USER_INFO="haiadmin:10020:xxxxxxxx" # platform user info, format: "user1:uid1:passwd,user2:uid2:passwd"
    export ROOT_USER="haiadmin" # username of the root user, must exist in $USER_INFO

    # optional, if not set, following default value will be used
    export BFF_ADMIN_UID=10000 # uid of the reserved admin user for bff
    export BFF_ADMIN_TOKEN=$(echo $RANDOM | md5sum | head -c 20)    # token of the reserved admin user for bff
    export BASE_IMAGE="registry.cn-hangzhou.aliyuncs.com/hfai/hai-platform:latest" # all in one image
    export TRAIN_IMAGE="registry.cn-hangzhou.aliyuncs.com/hfai/hai-platform:latest" # train image
    export NODE_GPUS=4 # gpus per node
    export HAS_RDMA_HCA_RESOURCE=0 # ib per node
    export KUBECONFIG="$HOME/.kube/config" # kubeconfig file path
    export POSTGRES_USER="root" # postgres username
    export POSTGRES_PASSWORD="root" # postgres password
    export REDIS_PASSWORD="root" # redis password
    export DB_PATH="${SHARED_FS_ROOT}/hai-platform/db" # postgres database path
    # export EXTRA_MOUNTS="src1:dst1:file:ro,src2:dst2:directory" # add extra mounts to hai-platform
    # export EXTRA_ENVIRONMENTS="k1:v1,k2:v2" # add extra environments to hai-platform

    # for k8s provider, optional, if not set, following default value will be used
    export INGRESS_CLASS="nginx" # ingress class
    export PLATFORM_NAMESPACE="${TASK_NAMESPACE}" # platform namespace

    # for docker-compose provider
    export HAI_SERVER_ADDR="47.98.195.232" # current server address'
}

confirm() {
    message="${1}. Continue? (Yy/Nn)"
    read -p "${message}" -n 1 -r
    [[ ! $REPLY =~ ^[Yy]$ ]] && { echo -e "\nCanceled."; exit 1; } || echo -e "\nContinue."
}

parse_user_info() {
  # input $USER_INFO: "user1:uid1:passwd,user2:uid2:passwd"
  # output $USER_INFO_DB: ('uid1', 'user1', 'passwd', 'internal', true, 'hfai'),('uid2', 'user2', 'passwd', 'internal', true, 'hfai')
  USER_INFO="${USER_INFO},bff_admin:${BFF_ADMIN_UID}:${BFF_ADMIN_TOKEN}"
  user_list=(${USER_INFO//,/ })
  for user in ${user_list[@]}; do
    user_info=(${user//:/ })
    USER_NAMES+=(${user_info[0]})
    USER_IDS+=(${user_info[1]})
    USER_TOKENS+=(${user_info[2]})
  done

  USER_INFO_DB=""
  user_count=${#USER_NAMES[@]}
  for (( i=0; i<${user_count}; i++)); do
    USER_INFO_DB+="('${USER_IDS[i]}', '${USER_NAMES[i]}', '${USER_TOKENS[i]}', 'internal', true, 'hfai'), "
  done
  USER_INFO_DB=${USER_INFO_DB::-2}
}

parse_host_info() {
  # input $TRAINING_NODES $JUPYTER_NODES
  TRAINING_NODES_ARRAY=($(for n in ${TRAINING_NODES[@]}; do echo $n; done | sort -u))
  JUPYTER_NODES_ARRAY=($(for n in ${JUPYTER_NODES[@]}; do echo $n; done | sort -u))
  TOTAL_NODES_ARRAY=()
  TOTAL_NODES_ARRAY+=(${TRAINING_NODES_ARRAY[@]})
  TOTAL_NODES_ARRAY+=(${JUPYTER_NODES_ARRAY[@]})
  TOTAL_NODES_DEDUP=($(for n in ${TOTAL_NODES_ARRAY[@]}; do echo $n; done | sort -u))
  echo "training nodes: ${TRAINING_NODES_ARRAY[@]}"
  echo "jupyter nodes: ${JUPYTER_NODES_ARRAY[@]}"
  if [[ ${#TOTAL_NODES_ARRAY[@]} != ${#TOTAL_NODES_DEDUP[@]} ]]; then
    echo "duplicated nodes: ${TOTAL_NODES_ARRAY[@]}, please fix"
    exit 1
  fi
  HOST_INFO_DB=""
  for n in ${TRAINING_NODES_ARRAY[@]}; do
    HOST_INFO_DB+="('${n}', ${NODE_GPUS}, 'gpu', 'training', '${TRAINING_GROUP}', 'A'), "
  done
  for n in ${JUPYTER_NODES_ARRAY[@]}; do
    HOST_INFO_DB+="('${n}', ${NODE_GPUS}, 'gpu', 'training', '${JUPYTER_GROUP}', 'A'), "
  done
  HOST_INFO_DB=${HOST_INFO_DB::-2}
}

parse_k8s_extra_mounts() {
  # input $EXTRA_MOUNTS: "src1:dst1:file:ro,src2:dst2:directory"
  # output: $EXTRA_K8S_HOSTPATH, $EXTRA_K8S_VOLUMEMOUNTS in k8s volume yaml format
  EXTRA_K8S_HOSTPATH=""
  EXTRA_K8S_VOLUMEMOUNTS=""
  mount_list=(${EXTRA_MOUNTS//,/ })
  i=0
  for item in ${mount_list[@]}; do
    item_info=(${item//:/ })
    if [[ ${item_info[2]} == "file" ]]; then
      item_type="File"
    else
      item_type="Directory"
    fi
    if [[ ${item_info[3]} == "ro" ]]; then
      read_only="true"
    else
      read_only="false"
    fi
    EXTRA_K8S_HOSTPATH+="      - hostPath:\n"
    EXTRA_K8S_HOSTPATH+="          path: ${item_info[0]}\n"
    EXTRA_K8S_HOSTPATH+="          type: ${item_type}\n"
    EXTRA_K8S_HOSTPATH+="        name: extra-${i}\n"

    EXTRA_K8S_VOLUMEMOUNTS+="        - mountPath: ${item_info[1]}\n"
    EXTRA_K8S_VOLUMEMOUNTS+="          name: extra-${i}\n"
    EXTRA_K8S_VOLUMEMOUNTS+="          readOnly: ${read_only}\n"
    ((i+=1))
  done
  echo -e "EXTRA_K8S_VOLUMEMOUNTS: \n${EXTRA_K8S_VOLUMEMOUNTS}"
  echo -e "EXTRA_K8S_HOSTPATH: \n${EXTRA_K8S_HOSTPATH}"
}

parse_docker_compose_extra_mounts() {
  # input $EXTRA_MOUNTS: "src1:dst1:file:ro,src2:dst2:directory"
  # output: $EXTRA_DOCKER_COMPOSE_VOLUMES in docker-compose volume yaml format
  EXTRA_DOCKER_COMPOSE_VOLUMES=""
  mount_list=(${EXTRA_MOUNTS//,/ })
  for item in ${mount_list[@]}; do
    item_info=(${item//:/ })
    if [[ ${item_info[3]} == "ro" ]]; then
      read_only="ro"
    else
      read_only="rw"
    fi
    EXTRA_DOCKER_COMPOSE_VOLUMES+="      - '${item_info[0]}:${item_info[1]}:${read_only}'\n"
  done
}

parse_k8s_extra_environments() {
  # input $EXTRA_ENVIRONMENTS: "k1:v1,k2:v2"
  # output: $EXTRA_K8S_ENVIRONMENTS in k8s yaml format
  EXTRA_K8S_ENVIRONMENTS=""
  environment_list=(${EXTRA_ENVIRONMENTS//,/ })
  for item in ${environment_list[@]}; do
    item_info=(${item//:/ })
    if [[ ${#item_info[@]} != 2 ]]; then
      echo "EXTRA_ENVIRONMENTS should be in format: 'k1:v1,k2:v2', please fix"
      exit 1
    fi
    EXTRA_K8S_ENVIRONMENTS+="        - name: ${item_info[0]}\n"
    EXTRA_K8S_ENVIRONMENTS+="          value: ${item_info[1]}\n"
  done
  echo -e "EXTRA_K8S_ENVIRONMENTS: \n${EXTRA_K8S_ENVIRONMENTS}"
}

parse_docker_compose_extra_environments() {
  # input $EXTRA_ENVIRONMENTS: "k1:v1,k2:v2"
  # output: $EXTRA_DOCKER_COMPOSE_ENVIRONMENTS in docker-compose yaml format
  EXTRA_DOCKER_COMPOSE_ENVIRONMENTS=""
  environment_list=(${EXTRA_ENVIRONMENTS//,/ })
  for item in ${environment_list[@]}; do
    item_info=(${item//:/ })
    if [[ ${#item_info[@]} != 2 ]]; then
      echo "EXTRA_ENVIRONMENTS should be in format: 'k1:v1,k2:v2', please fix"
      exit 1
    fi
    EXTRA_DOCKER_COMPOSE_ENVIRONMENTS+="      ${item_info[0]}: ${item_info[1]}\n"
  done
}

backup() {
  if [[ -f $1 ]]; then
    mv $1 $1.old
  fi
}

generate_manifest() {
  mkdir -p ${HAI_PLATFORM_PATH}
  cat << \EOF > ${HAI_PLATFORM_PATH}/init.sql.in
INSERT INTO "storage" ("host_path", "mount_path", "owners", "conditions", "mount_type", "read_only", "action", "active")
VALUES
      -- marsv2 的脚本, 运行的依赖，不要改
      ('marsv2-scripts-{task.id}:suspend_helper.py', '/marsv2/scripts/suspend_helper.py', '{public}', '{}'::varchar[], 'configmap', true, 'add', true),
      ('marsv2-scripts-{task.id}:task_log_helper.py', '/marsv2/scripts/task_log_helper.py', '{public}', '{}'::varchar[], 'configmap', true, 'add', true),
      ('marsv2-scripts-{task.id}:stop_helper.py', '/marsv2/scripts/stop_helper.py', '{public}', '{}'::varchar[], 'configmap', true, 'add', true),
      ('marsv2-scripts-{task.id}:validate_image.sh', '/marsv2/scripts/validate_image.sh', '{public}', '{}'::varchar[], 'configmap', true, 'add', true),
      ('marsv2-scripts-{task.id}:waiting_for_master.sh', '/marsv2/scripts/waiting_for_master.sh', '{public}', '{}'::varchar[], 'configmap', true, 'add', true),
      ('marsv2-scripts-{task.id}:waiting_pods_done.py', '/marsv2/scripts/waiting_pods_done.py', '{public}', '{}'::varchar[], 'configmap', true, 'add', true),
      ('marsv2-scripts-{task.id}:start_jupyter_with_ext.sh', '/marsv2/scripts/start_jupyter_with_ext.sh', '{public}', '{}'::varchar[], 'configmap', true, 'add', true),
      ('marsv2-scripts-{task.id}:hf_login_handler.py', '/marsv2/scripts/hf_login_handler.py', '{public}', '{}'::varchar[], 'configmap', true, 'add', true),
      ('marsv2-scripts-{task.id}:hf_kernel_spec_manager.py', '/marsv2/scripts/hf_kernel_spec_manager.py', '{public}', '{}'::varchar[], 'configmap', true, 'add', true),
      ('marsv2-entrypoints-{task.id}', '/marsv2/entrypoints', '{public}', '{}'::varchar[], 'configmap', true, 'add', true)
ON CONFLICT DO NOTHING;

INSERT INTO "storage" ("host_path", "mount_path", "owners", "conditions", "mount_type", "read_only", "action", "active")
VALUES
      -- 用户工作目录
      ('${HAI_PLATFORM_PATH}/workspace/{user.user_name}', '${HAI_PLATFORM_PATH}/workspace/{user.user_name}', '{public}', '{}'::varchar[], 'Directory', false, 'add', true)
ON CONFLICT DO NOTHING;

-- 和上面创建的分组对应起来 training
INSERT INTO "quota" ("user_name", "resource", "quota")
VALUES
      -- 设置的分组和权限，public 为所有用户，权限一共有 7 级
      -- LOW, BELOW_NORMAL, NORMAL, ABOVE_NORMAL, HIGH, VERY_HIGH, EXTREME_HIGH
      ('public', 'node-${TRAINING_GROUP}-HIGH', 10),
      -- 设置可用的训练节点数量
      ('public', 'node', 1),
      -- 设置可用的 nodeport 数量
      ('public', 'port', 1),
      -- 系统内建镜像，和下面的 train_environment 对应起来
      ('public', 'train_environment:hai_base', 1),
      -- 任务容器的capabilities
      ('public', 'cap:IPC_LOCK', 1),
      -- jupyter任务的分组
      ('public', 'jupyter:${JUPYTER_GROUP}', 100012810)
ON CONFLICT DO NOTHING;

INSERT INTO "user" ("user_id", "user_name", "token", "role", "active", "shared_group")
VALUES
      -- 用户
      ${USER_INFO_DB}
ON CONFLICT DO NOTHING;

INSERT INTO "user_group" ("user_name", "group")
VALUES
    -- 用户的权限组
    ('${ROOT_USER}', 'root')
ON CONFLICT DO NOTHING;

INSERT INTO "train_environment" ("env_name", "image", "schema_template", "config")
VALUES
      -- 训练镜像，如自定义，需要满足 validate_image.sh 中镜像的条件
      ('hai_base', '${TRAIN_IMAGE}', '', '{"environments": {"BFF_URL": "http://${INGRESS_HOST}", "WS_URL": "ws://${INGRESS_HOST}", "CLUSTER_SERVER_URL": "http://${HAI_SERVER_ADDR}"}, "python": "/usr/bin/python3.8"}')
ON CONFLICT DO NOTHING;

INSERT INTO "host" ("node", "gpu_num", "type", "use", "origin_group", "room")
VALUES
      -- 机器信息
      ${HOST_INFO_DB}
ON CONFLICT DO NOTHING;
EOF

  cat << \EOF > ${HAI_PLATFORM_PATH}/override.toml.in
# 任务日志的位置
[[experiment.log.dist]]
role = 'internal'
dir = '${HAI_PLATFORM_PATH}/workspace/log/{user_name}'
# 数据库配置
[database.postgres.primary]
host = '${HAI_SERVER_ADDR}'
port = 5432
user = '${POSTGRES_USER}'
password = '${POSTGRES_PASSWORD}'
[database.postgres.secondary]
host = '${HAI_SERVER_ADDR}'
port = 5432
user = '${POSTGRES_USER}'
password = '${POSTGRES_PASSWORD}'
[database.redis]
host = '${HAI_SERVER_ADDR}'
port = 6379
db = 0
password = '${REDIS_PASSWORD}'
[scheduler]
default_group = '${TRAINING_GROUP}'  # 提交任务时候的调度默认分组
[launcher]
api_server = '${HAI_SERVER_ADDR}' # 设定 server 地址
manager_nodes = [${MANAGER_NODES_FORMATTED}] # manager 所在节点, 格式为['node1','node2']
manager_image = '${BASE_IMAGE}'  # manager 使用的镜像，和 hai image 一致即可
[launcher.task_namespaces_by_role]
# 任务运行的namespace
internal = '${TASK_NAMESPACE}'
external = '${TASK_NAMESPACE}'-external
# 下面配置 manager 使用的 kube config
[launcher.manager_envs]
KUBECONFIG = '/root/.kube/config'
HAS_RDMA_HCA_RESOURCE = '${HAS_RDMA_HCA_RESOURCE}'
[launcher.manager_mounts]
kubeconfig = '${HAI_PLATFORM_PATH}/kubeconfig:/root/.kube'

[jupyter]
shared_node_group_prefix='${JUPYTER_GROUP}'
[jupyter.builtin_services.jupyter.environ.internal]
JUPYTER_DIR = '${HAI_PLATFORM_PATH}/workspace/{user_name}/jupyter'
[jupyter.builtin_services.jupyter.environ.external]
JUPYTER_DIR = '${HAI_PLATFORM_PATH}/workspace/{user_name}/jupyter'
[jupyter.ingress_host]
internal = '${INGRESS_HOST}'
external = '${INGRESS_HOST}'
studio = '${INGRESS_HOST}'
EOF

  if [[ ${PROVIDER} == "docker-compose" ]]; then
    cat << EOF > ${HAI_PLATFORM_PATH}/docker-compose.yaml.in
version: '3.3'
services:
  hai-platform:
    ports:
      # 记得端口下面的配置要用的
      - '80:80'     # http server 端口
      - '5432:5432' # pgsql
      - '6379:6379' # redis
      - '8080:8080' # studio
    volumes:
      - '${HAI_PLATFORM_PATH}/kubeconfig:/root/.kube:ro'                        # k8s config，必须挂载
      - '${HAI_PLATFORM_PATH}/log:/high-flyer/log'                              # platform 日志
      - '${DB_PATH}:/var/lib/postgresql/12/main'                                # 持久化 pgsql
      - '${HAI_PLATFORM_PATH}/redis:/var/lib/redis'                             # 持久化 redis
      - '${HAI_PLATFORM_PATH}/workspace:${HAI_PLATFORM_PATH}/workspace'         # 保存任务日志和用户工作目录
      - '${HAI_PLATFORM_PATH}/log/postgresql:/var/log/postgresql'               # postgres 日志
      - '${HAI_PLATFORM_PATH}/log/redis:/var/log/redis'                         # redis 日志
      - '${HAI_PLATFORM_PATH}/init.sql:/tmp/init.sql'                           # 初始化的数据库语句，在这里添加相关的帐号、依赖等等配置
      - '${HAI_PLATFORM_PATH}/override.toml:/etc/hai_one_config/override.toml'  # override配置
`echo -e "${EXTRA_DOCKER_COMPOSE_VOLUMES}"`
    environment:
      KUBECONFIG: /root/.kube/config
      PG_DATA_DIR: /var/lib/postgresql/12/main  # 记得和 thirdparty 里面的 postgresql.conf 对应起来
      # 分组信息，可以在这里创建，也可以自行在外面创建
      # 通过环境变量传入
      MARS_PREFIX: ${MARS_PREFIX}   # k8s label 的环境变量，会设置 ${MARS_PREFIX}_mars_group 为 ${HAI_GROUP}
      TRAINING_GROUP: ${TRAINING_GROUP}
      JUPYTER_GROUP: ${JUPYTER_GROUP}
      TRAINING_NODES: ${TRAINING_NODES}
      JUPYTER_NODES: ${JUPYTER_NODES}  # 使用空格分开
      HAI_SERVER_ADDR: ${HAI_SERVER_ADDR}
      INGRESS_HOST: ${INGRESS_HOST}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      REDIS_PASSWORD: ${REDIS_PASSWORD}
      BFF_ADMIN_TOKEN: ${BFF_ADMIN_TOKEN}
      BFF_ADDR: "${HAI_SERVER_ADDR}:8080"
`echo -e "${EXTRA_DOCKER_COMPOSE_ENVIRONMENTS}"`
    # logging:
    #   options:
    #     max-size: 1g
    image: ${BASE_IMAGE}
    # network_mode: "host"
  # 直接运行
    command: >
      bash -c "cd /high-flyer/code/multi_gpu_runner_server && bash one/entrypoint.sh"
EOF
  else
    mkdir -p ${HAI_PLATFORM_PATH}/k8s_configs
    cat << EOF > ${HAI_PLATFORM_PATH}/k8s_configs/hai-platform-deploy.yaml.in
apiVersion: v1
data:
  postgres.user: `echo -n "${POSTGRES_USER}" | base64`
  postgres.password: `echo -n "${POSTGRES_PASSWORD}" | base64`
  redis.password: `echo -n "${REDIS_PASSWORD}" | base64`
  bff.admintoken: `echo -n "${BFF_ADMIN_TOKEN}" | base64`
kind: Secret
metadata:
  labels:
    app: hai-platform
  name: hai-platform-secret
  namespace: ${PLATFORM_NAMESPACE}
type: Opaque
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  labels:
    app: hai-platform
  name: hai-platform
  namespace: ${PLATFORM_NAMESPACE}
spec:
  podManagementPolicy: OrderedReady
  replicas: 1
  selector:
    matchLabels:
      app: hai-platform
  serviceName: default
  updateStrategy:
    rollingUpdate:
      partition: 0
    type: RollingUpdate
  template:
    metadata:
      labels:
        app: hai-platform
    spec:
      # initContainers:
      # - name: init-chmod
      #   image: ${BASE_IMAGE}
      #   imagePullPolicy: Always
      #   command: ["bash", "-c", "chmod 777 /var/log/postgresql /var/log/redis"]
      #   volumeMounts:
      #   - mountPath: /var/log/postgresql
      #     name: pglog
      #   - mountPath: /var/log/redis
      #     name: redislog
      containers:
      - command:
        - bash
        - -c
        - cd /high-flyer/code/multi_gpu_runner_server && bash one/entrypoint.sh
        image: ${BASE_IMAGE}
        imagePullPolicy: Always
        name: hai-platform
        env:
        - name: KUBECONFIG
          value: /root/.kube/config
        - name: PG_DATA_DIR
          value: /var/lib/postgresql/12/main
        - name: MARS_PREFIX
          value: "${MARS_PREFIX}"
        - name: TRAINING_GROUP
          value: "${TRAINING_GROUP}"
        - name: JUPYTER_GROUP
          value: "${JUPYTER_GROUP}"
        - name: TRAINING_NODES
          value: "${TRAINING_NODES}"
        - name: JUPYTER_NODES
          value: "${JUPYTER_NODES}"
        - name: HAI_SERVER_ADDR
          value: "HAI_SERVER_ADDR_PLACE_HOLDER"
        - name: INGRESS_HOST
          value: "${INGRESS_HOST}"
        - name: BFF_ADDR
          value: "${INGRESS_HOST}"
        - name: POSTGRES_USER
          valueFrom:
            secretKeyRef:
              name: hai-platform-secret
              key: postgres.user
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: hai-platform-secret
              key: postgres.password
        - name: REDIS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: hai-platform-secret
              key: redis.password
        - name: BFF_ADMIN_TOKEN
          valueFrom:
            secretKeyRef:
              name: hai-platform-secret
              key: bff.admintoken
`echo -e "${EXTRA_K8S_ENVIRONMENTS}"`
        livenessProbe:
          failureThreshold: 5
          tcpSocket:
            port: 80
          initialDelaySeconds: 90
          periodSeconds: 30
          successThreshold: 1
          timeoutSeconds: 5
        readinessProbe:
          failureThreshold: 3
          tcpSocket:
            port: 80
          initialDelaySeconds: 30
          periodSeconds: 10
          successThreshold: 1
          timeoutSeconds: 5
        ports:
        - containerPort: 80
          name: web
          protocol: TCP
        - containerPort: 8080
          name: studio
          protocol: TCP
        - containerPort: 5432
          name: postgres
          protocol: TCP
        - containerPort: 6379
          name: redis
          protocol: TCP
        resources:
          requests:
            cpu: 8
            memory: 4Gi
        volumeMounts:
        - mountPath: /root/.kube
          name: kubeconfig
          readOnly: true
        - mountPath: /high-flyer/log
          name: log
        - mountPath: /var/lib/postgresql/12/main
          name: db
        - mountPath: /var/lib/redis
          name: redis
        - mountPath: ${HAI_PLATFORM_PATH}/workspace
          name: workspace
        - mountPath: /var/log/postgresql
          name: pglog
        - mountPath: /var/log/redis
          name: redislog
        - mountPath: /tmp/init.sql
          name: initsql
        - mountPath: /etc/hai_one_config/override.toml
          name: overridetoml
`echo -e "${EXTRA_K8S_VOLUMEMOUNTS}"`
      nodeSelector:
        kubernetes.io/os: linux
      priorityClassName: system-cluster-critical
      restartPolicy: Always
      terminationGracePeriodSeconds: 30
      volumes:
      - hostPath:
          path: ${HAI_PLATFORM_PATH}/kubeconfig
          type: Directory
        name: kubeconfig
      - hostPath:
          path: ${HAI_PLATFORM_PATH}/log
          type: Directory
        name: log
      - hostPath:
          path: ${DB_PATH}
          type: Directory
        name: db
      - hostPath:
          path: ${HAI_PLATFORM_PATH}/redis
          type: Directory
        name: redis
      - hostPath:
          path: ${HAI_PLATFORM_PATH}/workspace
          type: Directory
        name: workspace
      - hostPath:
          path: ${HAI_PLATFORM_PATH}/log/postgresql
          type: Directory
        name: pglog
      - hostPath:
          path: ${HAI_PLATFORM_PATH}/log/redis
          type: Directory
        name: redislog
      - hostPath:
          path: ${HAI_PLATFORM_PATH}/init.sql
          type: File
        name: initsql
      - hostPath:
          path: ${HAI_PLATFORM_PATH}/override.toml
          type: File
        name: overridetoml
`echo -e "${EXTRA_K8S_HOSTPATH}"`
EOF
    cat << EOF > ${HAI_PLATFORM_PATH}/k8s_configs/hai-platform-service.yaml.in
apiVersion: v1
kind: Service
metadata:
  labels:
    app: hai-platform
  name: hai-platform-svc
  namespace: ${PLATFORM_NAMESPACE}
spec:
  ports:
  - name: pgsql
    port: 5432
    protocol: TCP
    targetPort: 5432
  - name: redis
    port: 6379
    protocol: TCP
    targetPort: 6379
  - name: server
    port: 80
    protocol: TCP
    targetPort: 80
  - name: studio
    port: 8080
    protocol: TCP
    targetPort: 8080
  selector:
    app: hai-platform
  type: LoadBalancer
EOF
    cat << EOF > ${HAI_PLATFORM_PATH}/k8s_configs/hai-platform-ingress-studio.yaml.in
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: hai-platform-ingress-studio
  namespace: ${PLATFORM_NAMESPACE}
spec:
  ingressClassName: ${INGRESS_CLASS}
  rules:
  - host: ${INGRESS_HOST}
    http:
      paths:
      - backend:
          service:
            name: hai-platform-svc
            port:
              name: studio
        path: /
        pathType: ImplementationSpecific
EOF
  fi

  TEMPLATES=("init.sql" "override.toml")
  if [[ ${PROVIDER} == "docker-compose" ]]; then
    TEMPLATES+=("docker-compose.yaml")
  else
    TEMPLATES+=(
        "k8s_configs/hai-platform-deploy.yaml" "k8s_configs/hai-platform-service.yaml"
        "k8s_configs/hai-platform-ingress-studio.yaml"
    )
  fi

  for template in ${TEMPLATES[@]}; do
    backup ${HAI_PLATFORM_PATH}/${template}
    # pass fake HAI_SERVER_ADDR, it will be set after service provisioned
    if [[ ${PROVIDER} == "k8s" ]]; then
      HAI_SERVER_ADDR=HAI_SERVER_ADDR_PLACE_HOLDER
    fi
    envsubst < ${HAI_PLATFORM_PATH}/${template}.in > ${HAI_PLATFORM_PATH}/${template}
    rm ${HAI_PLATFORM_PATH}/${template}.in
  done
  echo "generated rendered config in ${HAI_PLATFORM_PATH}: ${TEMPLATES[@]}"
}


#################### STEP ####################

precheck() {
  # step: precheck
  print_step "STEP: precheck"
  if [[ ! -z "$DRY_RUN" ]]; then
    return
  fi
  if [[ ${PROVIDER} == "docker-compose" ]]; then
    if ! `docker ps > /dev/null 2>&1`; then echo "failed: docker not installed."; exit 1; fi
    [[ ! `which docker-compose` > /dev/null ]] && { echo "failed: docker-compose binary DOES NOT exists."; exit 1; }
    [[ ! -d ${SHARED_FS_ROOT} ]] && { echo "failed: SHARED_FS_ROOT ${SHARED_FS_ROOT} DOES NOT exists."; exit 1; }
    [[ -d ${HAI_PLATFORM_PATH} ]] && confirm "HAI_PLATFORM_PATH ${HAI_PLATFORM_PATH} already exists"
  fi

  [[ ! `which kubectl` > /dev/null ]] && { echo "failed: kubectl binary DOES NOT exists."; exit 1; }
  [[ ! -f ${KUBECONFIG} ]] && { echo "failed: KUBECONFIG ${KUBECONFIG} DOES NOT exists."; exit 1; }
  echo "precheck passed"
}

parse_env() {
  # 解析 MANAGER_NODES 格式为 'node1','node2'
  MANAGER_NODES_FORMATTED=""
  for n in ${MANAGER_NODES[@]}; do
    MANAGER_NODES_FORMATTED+="'$n',"
  done
  MANAGER_NODES_FORMATTED=${MANAGER_NODES_FORMATTED::-1}

  # step: parse user/host info
  print_step "STEP: parse user/host info"
  parse_user_info
  parse_host_info
  if [[ ${PROVIDER} == "docker-compose" ]]; then
    parse_docker_compose_extra_mounts
    parse_docker_compose_extra_environments
  else
    parse_k8s_extra_mounts
    parse_k8s_extra_environments
  fi

  # step: render config
  print_step "STEP: generate config"
  generate_manifest

  unset POSTGRES_PASSWORD
  unset REDIS_PASSWORD
  unset USER_INFO
  unset USER_INFO_DB

  if [[ ! -z "$DRY_RUN" ]]; then
    if [[ ${PROVIDER} == "docker-compose" ]]; then
      echo -e "  you can run hai-platform via:"
      echo -e "    docker-compose -f ${HAI_PLATFORM_PATH}/docker-compose.yaml pull"
      echo -e "    docker-compose -f ${HAI_PLATFORM_PATH}/docker-compose.yaml up -d"
    fi
    exit 0
  fi
}

create_workspace() {
  # step: prepare shared fs directory
  print_step "STEP: prepare shared fs platform directory"
  mkdir -p ${HAI_PLATFORM_PATH}/kubeconfig
  mkdir -p ${HAI_PLATFORM_PATH}/log
  mkdir -p ${HAI_PLATFORM_PATH}/log/postgresql
  mkdir -p ${HAI_PLATFORM_PATH}/log/redis
  chmod 777 ${HAI_PLATFORM_PATH}/log/postgresql ${HAI_PLATFORM_PATH}/log/redis
  mkdir -p ${DB_PATH}
  mkdir -p ${HAI_PLATFORM_PATH}/redis
  mkdir -p ${HAI_PLATFORM_PATH}/workspace
  mkdir -p ${HAI_PLATFORM_PATH}/workspace/log # 任务日志

  # step: add user directory
  print_step "STEP: prepare shared fs user directory"
  user_count=${#USER_NAMES[@]}
  for (( i=0; i<${user_count}; i++)); do
    mkdir -p ${HAI_PLATFORM_PATH}/workspace/${USER_NAMES[i]}
    mkdir -p ${HAI_PLATFORM_PATH}/workspace/${USER_NAMES[i]}/jupyter
    chown -R ${USER_IDS[i]}.${USER_IDS[i]} ${HAI_PLATFORM_PATH}/workspace/${USER_NAMES[i]}
  done
}

create_kubeconfig() {
  # step: prepare kubeconfig, rbac
  print_step "STEP: prepare kubeconfig, rbac"
  cp ${KUBECONFIG} ${HAI_PLATFORM_PATH}/kubeconfig/

  if ! `kubectl --kubeconfig ${KUBECONFIG} get ns ${TASK_NAMESPACE} > /dev/null`; then
    kubectl --kubeconfig ${KUBECONFIG} create ns ${TASK_NAMESPACE}
  fi
  echo "
  apiVersion: rbac.authorization.k8s.io/v1
  kind: ClusterRoleBinding
  metadata:
    name: ${TASK_NAMESPACE}
  roleRef:
    apiGroup: rbac.authorization.k8s.io
    kind: ClusterRole
    name: cluster-admin
  subjects:
  - kind: ServiceAccount
    name: default
    namespace: ${TASK_NAMESPACE}
  " | kubectl --kubeconfig ${KUBECONFIG} apply -f -

  if [[ ${TASK_NAMESPACE} != ${PLATFORM_NAMESPACE} ]]; then
    if ! `kubectl --kubeconfig ${KUBECONFIG} get ns ${PLATFORM_NAMESPACE} > /dev/null`; then
      kubectl --kubeconfig ${KUBECONFIG} create ns ${PLATFORM_NAMESPACE}
    fi
    echo "
    apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRoleBinding
    metadata:
      name: ${PLATFORM_NAMESPACE}
    roleRef:
      apiGroup: rbac.authorization.k8s.io
      kind: ClusterRole
      name: cluster-admin
    subjects:
    - kind: ServiceAccount
      name: default
      namespace: ${PLATFORM_NAMESPACE}
    " | kubectl --kubeconfig ${KUBECONFIG} apply -f -
  fi
}

setup() {
  print_step "STEP: start hai-platform"
  if [[ ${PROVIDER} == "docker-compose" ]]; then
    docker-compose -f ${HAI_PLATFORM_PATH}/docker-compose.yaml pull
    docker-compose -f ${HAI_PLATFORM_PATH}/docker-compose.yaml up -d
  else
    # for k8s provider, create service first to obtain HAI_SERVER_ADDR
    kubectl --kubeconfig ${KUBECONFIG} apply -f ${HAI_PLATFORM_PATH}/k8s_configs/hai-platform-service.yaml
    echo "waiting for loadbalancer provision..."
    for i in {1..180}; do
      HAI_SERVER_ADDR=`kubectl --kubeconfig ${KUBECONFIG} -n ${PLATFORM_NAMESPACE} get svc hai-platform-svc -o jsonpath='{.status.loadBalancer.ingress[0].ip}'`
      if [[ $? != 0 ]]; then continue; fi
      if [[ ${HAI_SERVER_ADDR} != "" ]]; then
        echo "loadbalancer provisioned, HAI_SERVER_ADDR: ${HAI_SERVER_ADDR}"
        break
      fi
      if [[ ${i} == 180 ]]; then
        echo "loadbalancer provision timeout!"
        exit 1
      fi
      sleep 5
    done
    TEMPLATES=("init.sql" "override.toml" "k8s_configs/hai-platform-deploy.yaml" "k8s_configs/hai-platform-ingress-studio.yaml")
    for template in ${TEMPLATES[@]}; do
      sed -i "s/HAI_SERVER_ADDR_PLACE_HOLDER/${HAI_SERVER_ADDR}/" ${HAI_PLATFORM_PATH}/${template}
    done
    kubectl --kubeconfig ${KUBECONFIG} apply -f ${HAI_PLATFORM_PATH}/k8s_configs
  fi
}

teardown() {
    print_step "STEP: tear down"
    if [[ ${PROVIDER} == "docker-compose" ]]; then
      confirm "tear down using config ${HAI_PLATFORM_PATH}/docker-compose.yaml"
      docker-compose -f ${HAI_PLATFORM_PATH}/docker-compose.yaml down
    else
      confirm "tear down using config ${HAI_PLATFORM_PATH}/k8s_configs"
      kubectl --kubeconfig ${KUBECONFIG} delete -f ${HAI_PLATFORM_PATH}/k8s_configs
    fi
    echo "tear down succeed."
}

#################### START HERE ####################

if [ $# == 0 ]; then
  print_usage
  exit 0
fi

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -h | --help)
      print_usage
      exit 0
      ;;
    -p | --provider)
      PROVIDER=$2;
      if [[ ${PROVIDER} != "k8s" && ${PROVIDER} != "docker-compose" ]]; then
        echo "unsupported provider ${PROVIDER}!"
        exit 1
      fi
      shift
      ;;
    -c | --config)
      config_file=$2;
      source ${config_file}
      shift
      ;;
    dryrun)
      echo "DRYRUN."
      DRY_RUN=y
      ;;
    run | up)
      ;;
    down)
      TEARDOWN="true"
      ;;
    config)
      print_config_script
      exit 0
      ;;
    upgrade)
      pip install hai --extra-index-url https://pypi.hfai.high-flyer.cn/simple --trusted-host pypi.hfai.high-flyer.cn -U --no-cache --force-reinstall
      exit 0
      ;;
    *)
      print_usage
      exit 1
      ;;
  esac
  shift
done

echo "Using provider: $PROVIDER"

print_step "################################"
print_step "#   WELCOME TO HAI PLATFORM!   #"
print_step "################################"

set_env
if [[ ${TEARDOWN} == "true" ]]; then
  teardown
  exit 0
fi
precheck
parse_env
create_workspace
create_kubeconfig
setup

print_step "Setup hai-platform succeed, you can see the container status via:"
if [[ ${PROVIDER} == "docker-compose" ]]; then
  echo "  docker ps | grep hai-platform"
else
  echo "  kubectl --kubeconfig ${KUBECONFIG} -n ${PLATFORM_NAMESPACE} get po | grep hai-platform"
fi
