# HAI Platform

High-flyer AI 的分时调度训练平台, 可通过 `docker-compose` 或 `k8s` 部署，提供功能：
- 训练任务分时调度
- 训练任务管理
- jupyter 开发容器管理
- [studio用户界面](https://github.com/HFAiLab/hai-platform-studio)
- haienv 运行环境管理

## 外部依赖
1. 一个集中存储，如 `nfs`, `ceph`, `weka`
    - 用于存放用户的运行代码
    - 代码运行输出的日志
    - 部署需要的 k8s conf
2. k8s，确保你的运算主机都在 k8s 集群中
3. 建议计算节点支持rdma，并安装 [rdma-sriov device-plugin](https://github.com/mellanox/k8s-rdma-shared-dev-plugin)
   - 如果没有，在 配置项目 `launcher.manager_envs` 中配置 `HAS_RDMA_HCA_RESOURCE: '0'`

## 快速上手
1. 构建

    构建 all-in-one hai-platform 镜像

    注：如需包含 haienv 202207 运行环境（包含cuda, torch），以同时作为训练任务镜像，需 `export BUILD_TRAIN_IMAGE=1`；如需自定义训练任务镜像，请参考 [附录：初始化数据库](#初始化数据库) 中 `train_environment` 的配置说明。
    ```shell
    # replace IMAGE_REPO with your own repo
    $ IMAGE_REPO=registry.cn-hangzhou.aliyuncs.com/hfai/hai-platform bash one/release.sh
      build hai success:
        hai-platform image: registry.cn-hangzhou.aliyuncs.com/hfai/hai-platform:fa07f13
        hai-cli whl:
          /home/hai-platform/build/hai-1.0.0+fa07f13-py3-none-any.whl
          /home/hai-platform/build/haienv-1.4.1+fa07f13-py3-none-any.whl
          /home/hai-platform/build/haiworkspace-1.0.0+fa07f13-py3-none-any.whl
    ```

    安装 hai-cli 命令行
    ```shell
    pip3 install /home/hai-platform/build/hai-1.0.0+fa07f13-py3-none-any.whl
    pip3 install /home/hai-platform/build/haienv-1.4.1+fa07f13-py3-none-any.whl
    pip3 install /home/hai-platform/build/haiworkspace-1.0.0+fa07f13-py3-none-any.whl
    ```

    也可以使用预构建的镜像和命令行：
    ```shell
    # 仅包含 hai-platform
    registry.cn-hangzhou.aliyuncs.com/hfai/hai-platform:latest
    # 包含 hai-platform 和 haienv 202207 运行环境（包含cuda, torch）
    registry.cn-hangzhou.aliyuncs.com/hfai/hai-platform:latest-202207

    pip3 install hai --extra-index-url https://pypi.hfai.high-flyer.cn/simple --trusted-host pypi.hfai.high-flyer.cn -U
    ```

2. 部署 hai-platform 到 k8s 集群

- 获取使用帮助
  ```shell
  $ hai-up -h
  Usage:
    hai-up.sh config/run/up/dryrun/down/upgrade [option]
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

  Setup guide
    step 1: ensure the following dependencies satisfied
      - a kubernetes cluster with loadbalancer and ingress supported
      - a shared filesystem mounted in current host and other compute nodes
      - for provider docker-compose: docker and docker-compose should be installed in current host
    step 2: "hai-up config > config.sh", modify environment variables in config.sh
    step 3: "hai-up run -c config.sh" to start the all-in-one hai-platform.
  ```

- 按提示配置相关环境变量，如共享文件系统路径、节点分组信息、训练镜像、用户信息、挂载点等，确认无误后部署（如需自定义更多配置，可运行 `hai-up dryrun` 获取配置模板并自行修改，手动部署）
  ```shell
  hai-up config > config.sh
  # customize config.sh

  hai-up run -c config.sh
  ```

- 使用 `hai-cli` 初始化和提交任务
  ```shell
  HAI_SERVER_ADDR=`kubectl -n hai-platform get svc hai-platform-svc -o jsonpath='{.status.loadBalancer.ingress[0].ip}'`

  # TOKEN 为 USER_INFO 设置的token
  hai-cli init ${TOKEN} --url http://${HAI_SERVER_ADDR}

  # python文件默认需放在用户工作目录 ${SHARED_FS_ROOT}/hai-platform/workspace/{user.user_name}
  # 如置于其他路径，需要在pg数据库storage表中添加相应挂载项
  hai-cli python ${SHARED_FS_ROOT}/hai-platform/workspace/$(whoami)/test.py -- -n 1
  ```

- 如需停用hai-platform，运行 `hai-up down`


## 附录：配置说明
`hai-up run/dryrun` 会创建 init.sql, override.toml 配置文件，以及部署到 k8s/docker-compose 的yaml文件，如有自定义配置需求，可自行修改。部分配置解释如下。

### 网络端口
默认打开如下端口：
- 80:   server
- 5432: postgresql
- 6379: redis
- 8080: studio
1. hai-platform 将启动一个 webservice，监听在 80 端口
2. 内建的 pgsql 和 redis 需要对外访问，让 manager 能够访问到
   - 用户如果用自己搭建的外部 pgsql 和 redis，那么后两个端口可以不设置
3. studio 为平台提供管理 UI 页面

### k8s 相关配置
平台需要在集群创建 deployment,statefulset,pod,configmap 等资源，需要通过 rbac 为平台账号授权，或者直接赋予admin权限，对应的 kubeconfig 会被挂载到`hai-platform`的默认路径`/root/.kube/config`

### 节点分组
目前支持两类分组，TRAINING_GROUP, JUPYTER_GROUP，分别表示训练节点和jupyter节点。
需要指定分组名，以及分组内节点列表，如：
```shell
export MARS_PREFIX="hai-platform-one"
export TRAINING_GROUP="training"
export JUPYTER_GROUP="jupyter_cpu"
export TRAINING_NODES="cn-hangzhou.172.23.183.227"
export JUPYTER_NODES="cn-hangzhou.172.23.183.226"
```
设置分组信息后，启动脚本会自动配置 k8s node 的 label 为 `${MARS_PREFIX}_mars_group=${TRAINING_GROUP}` 或 `${MARS_PREFIX}_mars_group=${JUPYTER_GROUP}`; 并给 `hai-platform` 的 `schduler` 设置调度分组，同时设置数据库中的quota

### 挂载点
默认会挂载如下路径
```yaml
  - '${HAI_PLATFORM_PATH}/kubeconfig:/root/.kube:ro'                        # k8s config，必须挂载
  - '${HAI_PLATFORM_PATH}/log:/high-flyer/log'                              # platform 日志
  - '${DB_PATH}:/var/lib/postgresql/12/main'                                # 持久化 pgsql
  - '${HAI_PLATFORM_PATH}/redis:/var/lib/redis'                             # 持久化 redis
  - '${HAI_PLATFORM_PATH}/workspace/log:${HAI_PLATFORM_PATH}/workspace/log' # 保存任务日志
  - '${HAI_PLATFORM_PATH}/log/postgresql:/var/log/postgresql'               # postgres 日志
  - '${HAI_PLATFORM_PATH}/log/redis:/var/log/redis'                         # redis 日志
  - '${HAI_PLATFORM_PATH}/init.sql:/tmp/init.sql'                           # 初始化的数据库语句，在这里添加相关的帐号、依赖等等配置
  - '${HAI_PLATFORM_PATH}/override.toml:/etc/hai_one_config/override.toml'  # override配置
```
1. 挂载 k8s 配置文件
2. 若需要，可以挂载 pgsql 的目录作为持久化目录，注意，容器内配置了 `/var/lib/postgresql/12/main` 作为数据目录
3. 用户的任务日志目录，需要在共享文件系统上，该目录信息会被配置到 `override.toml` 的 `experiment.log.dist` 中

### 数据库配置
#### 内建数据库
镜像内建了 `pgsql` 和 `redis`，并且通过 `override.toml` 配置相关信息。
```yaml
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
```

#### 初始化数据库
`hai-platfrom` 的用户信息，以及集群挂载信息都在 postgres 数据库中，可以通过数据库来控制用户的 quota、storage、权限等等，
初始化时会执行一次 init.sql, 初始化后如需修改数据库信息，请直接到数据中更改。
`init.sql` 模板示例如下：
```yaml
INSERT INTO "storage" ("host_path", "mount_path", "owners", "conditions", "mount_type", "read_only", "action", "active")
VALUES
      -- marsv2 的脚本, 运行的依赖，不要改
      ('marsv2-scripts-{task.id}:suspend_helper.py', '/marsv2/scripts/suspend_helper.py', '{public}', '{}'::varchar[], 'configmap', true, 'add', true),
      ('marsv2-scripts-{task.id}:task_log_helper.py', '/marsv2/scripts/task_log_helper.py', '{public}', '{}'::varchar[], 'configmap', true, 'add', true),
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
      ('10020', 'testuser', 'xxxxxxxx', 'internal', true, 'hfai')
ON CONFLICT DO NOTHING;

INSERT INTO "train_environment" ("env_name", "image", "schema_template", "config")
VALUES
      -- 训练镜像，如自定义，需要满足 validate_image.sh 中镜像的条件
      ('hai_base', '${TRAIN_IMAGE}', '', '{"environments": {}, "python": "/usr/bin/python3.8"}')
ON CONFLICT DO NOTHING;

INSERT INTO "host" ("node", "gpu_num", "type", "use", "origin_group", "room")
VALUES
      -- 机器信息
      ('cn-hangzhou.172.23.183.226', 4, 'gpu', 'training', '${TRAINING_GROUP}', 'A'),
      ('cn-hangzhou.172.23.183.227', 4, 'gpu', 'training', '${TRAINING_GROUP}', 'A')
ON CONFLICT DO NOTHING;
```

### 平台配置
平台配置为 `toml` 格式，镜像中已经默认做了大量配置，用户只需要做基本的个性化配置即可，通过override.toml来覆盖镜像默认配置。
`override.toml` 示例：
```toml
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
  task_namespace = '${TASK_NAMESPACE}'     # 任务运行的namespace
  manager_nodes = [${MANAGER_NODES_FORMATTED}] # manager 所在节点, 格式为['node1','node2']
  manager_image = '${BASE_IMAGE}'  # manager 使用的镜像，和 hai image 一致即可
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
  hfhub = '${INGRESS_HOST}'
  yinghuo = '${INGRESS_HOST}'
  studio = '${INGRESS_HOST}'
```

### SSH 配置

开发容器及 `hai-cli` 都提供了 SSH 相关的功能，但出于安全考虑目前任务容器中默认不启动 SSH 服务，也未为平台用户自动生成用于 SSH 连接的密钥。

如果想要使用相关功能，可以按照如下步骤配置：
1. 编写 SSH 服务的初始化脚本，用于在任务容器的初始化阶段完成配置 SSH Key、启动 SSH 服务等工作；
2. 修改数据库的 `storage` 表，将此脚本挂载至任务容器的 `/usr/local/sbin/hf-scripts/post_system_init/` 目录下，
以使其在任务容器的初始化阶段运行。关于此目录的更多信息，可以参考 `/marsv2/entrypoints` 中的任务初始化脚本。
