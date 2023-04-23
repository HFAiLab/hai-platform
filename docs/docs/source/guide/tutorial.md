# 命令行工具 (CLI)
命令行工具（hai cli）是用户访问 HAI Platform 进行操作的终端工具，其中包括便捷的调试接口、提交/查看任务的接口、任务操作/查看任务输出的接口等。在网络可访问的条件下，命令行工具可以在集群外的用户终端使用，同步用户指定环境及工作区。

键入 `hai-cli --help` 即可查看所有支持的功能。 

```shell
Usage: hai-cli COMMAND <argument>... [OPTIONS]

Options:
  --version   Show the version and exit.
  -h, --help  Show this message and exit.

Exec Commands:
  bash    在集群上运行 bash 脚本
  exec    在集群上运行 二进制 文件
  python  在集群上运行 bash 脚本
  run     根据 yaml 文件来运行一个任务，可以通过参数来覆盖配置;...

Task Manage Cmds:
  describe  打印任务的 schema yaml，可以在下次创建任务的时候使用
  list      列出用户任务列表, 用户要查看第几页的任务列表
  logs      查看任务日志
  ssh       登录到哪台机器，只能在开发容器内使用
  status    查询任务状态
  stop      关闭任务状态

Cluster Commands:
  monitor   获取当前任务列表相关信息
  nodes     查看节点信息
  prof      对正在运行中的任务进行 profile
  validate  检查节点正常情况
  version   显示版本信息

User Commands:
  init    初始化用户账户
  whoami  显示用户的个人信息, 包括集群用量、quota 等

UGC Commands:
  haienv       创建、查询、删除虚拟环境

```

本文给出常见场景下的用法示例。

## 用例1： 使用客户端工具的基本流程 
### 场景
用户开始使用 HAI Platform 平台，需要搭建个人运行环境、准备实验代码、数据等。集群开发容器默认安装并配置了 hai cli，用户也可以在个人本地安装。

### 说明
使用 hai 需要按用户初始化本地配置，支持用户创建个人虚拟环境，并且支持环境上传，确保本地和集群上环境一致。

**注意**：**目前仅支持 bash**, 如果要使用其它 shell，需要手动修改 venv 环境下的 activate 脚本。

### 步骤
1. 在本地首次安装使用，执行 [`hai-cli init <token>`](../cli/user.html#hai-cli-init) 命令初始化 hai 客户端配置。初始化过程会生成配置文件 `~/.hai/conf.yaml`。在集群开发容器中使用则无需重复初始化。

2. 使用 [`hai-cli whoami`](../cli/user.html#hai-cli-whoami) 查看用户个人信息，包括集群用量、配额等。

3. 使用 [`haienv`](../cli/ugc.html#haienv) 工具创建及管理个人环境：

    ```shell
    haienv create <env_name>  # 创建虚拟环境
    source haienv <env_name>  # 进入此虚拟环境
    pip install <your_package>  # 在此虚拟环境下安装依赖包
    conda install <your_package> # 在此虚拟环境下安装依赖包
    ```

    **注意**：在集群开发容器创建虚拟环境，可以选择继承集群基础环境，只做增量安装

### 示例
```shell
# 创建个人虚拟环境
haienv create extenv  ## 在基础环境上扩展，相同包可以覆盖基础环境里的版本
# 输出
#确认您要构建的虚拟环境版本为：3.8.10；扩展py38-202111环境
#Y/N: Y
#Collecting package metadata (current_repodata.json): ...working... done
#Solving environment: ...working... failed with repodata from current_repodata.json, will retry #with next repodata source.
#Collecting package metadata (repodata.json): ...working... done
#Solving environment: ...working... done
#......
#...
#...
#...
#Preparing transaction: ...working... done
#Verifying transaction: ...working... done
#Executing transaction: ...working... done
#创建虚拟环境成功，使用 source haienv extenv 进入

haienv list ## 列表查看虚拟环境
+-----------+-----------------------------------------+--------+-------------+--------+
| venv_name | path                                    | extend | extend_env  | py     |
+===========+=========================================+========+=============+========+
| extenv    | /hf_shared/hfai_envs/user/extenv_0      | True   | py38-202111 | 3.8.10 |
+-----------+-----------------------------------------+--------+-------------+--------+

haienv remove extenv ## 删除虚拟环境
```


## 用例2： 训练任务管理
### 场景
用户提交、查看、管理个人在集群上的任务。

### 说明
由于集群以[分时调度](schedule.md)的方式分配资源，每个任务实际上都是一个任务链(chain)。任务的 `chain_status` 有如下可能：
1. `waiting_init`: 该任务处于排队状态，尚未被调度到；
1. `running`: 该任务处于运行状态；
1. `suspended`: 该任务被调度器打断了，处于挂起状态；
1. `finished`: 该任务执行结束了。

另外，任务信息还包含运行节点信息，节点的状态有如下可能：
1. `created`: 该任务节点处于创建过程中；
1. `building`: 该任务节点处于初始化过程中；
1. `unschedulable`: 该任务节点在等待资源施放；
1. `running`: 该任务节点处于运行状态；
1. `succeeded_terminating` / `failed_terminating` / `stopped_terminating`: 该任务节点处于结束态（正在退出过程中），`_` 之前的描述词为其终态；
1. `succeeded` / `failed` / `stopped`: 该任务节点已经完全退出。

### 步骤
1. 执行 [`hai-cli run`](../cli/exec.html#hai-cli-run) 通过提交yaml文件来运行一个任务，支持运行python文件或shell脚本。yaml格式[参考API说明](../api/client.html#hfai.client.create_experiment_v2)。
   
   ```shell
   hai-cli run /path/to/yaml_file
   ```

   另外也可使用 [`hai-cli python`](../cli/exec.html#hai-cli-python), [`hai-cli bash`](../cli/exec.html#hai-cli-bash) 或 [`hai-cli exec`](../cli/exec.html#hai-cli-exec) 提交任务执行。

   ```shell
   hai-cli python <experiment.py> # 本地运行，等同于运行 python xxx
   hai-cli python <experiment.py> -- [CLUSTER_OPTIONS]  # 提交到集群作为新建任务运行，提交前请先检查工作区是否同步；集群任务参数详见api文档
   hai-cli python <experiment.py> ++ [SIMULATE_OPTIONS] # 在本地运行，模拟集群任务响应
   ```
2. 使用 [`hai-cli list`](../cli/task.html#hai-cli-list), [`hai-cli status`](../cli/task.html#hai-cli-status), [`hai-cli logs`](../cli/task.html#hai-cli-logs) 等查看任务状态、日志等。
3. 对于运行中或者等待中的任务，可以执行 [`hai-cli stop`](../cli/task.html#hai-cli-stop) 发起停止。用户停止的任务不再进入调度。

### 示例
通过 `hai-cli status <experiment>` 查看任务的状态
```shell
# 未特殊指明，<experiment> 均使用 experiment_name
hai-cli status sample_exp
# 输出
==================== experiment ====================
+--------+------------+-------+--------------+---------------+---------------------+
| id     | nb_name    | nodes | chain_status | suspend_count | updated_at          |
+========+============+=======+==============+===============+=====================+
| 397246 | sample_exp | 1     | finished     | 0             | 2021-09-23 11:04:20 |
+--------+------------+-------+--------------+---------------+---------------------+
==================== jobs ====================
+------+---------+-------------+---------------------+
| rank | status  | node        | started_at          |
+======+=========+=============+=====================+
| 0    | stopped | hfai-rank-0 | 2021-09-23T10:25:26 |
+------+---------+-------------+---------------------+

# 加 --json 参数可以将任务转换成 json 格式输出
hai-cli status sample_exp --json
```

通过 `hai-cli list` 功能查看个人历史任务。该接口带有分页功能，其中页大小（page_size）不能超过50。
```shell
hai-cli list [--page=<page>] [--page_size=<page_size>]
# 可以通过 --page 和 --page_size 参数来拿到指定范围的任务
# 获取第7-8个任务
hai-cli list --page 4 --page_size 2
# 输出
现在查看的是第 4 页任务，共 5 页, 每页 2 个任务, 共 9 个任务
+--------+------------+-------+--------------+---------------+---------------------+
| id     | nb_name    | nodes | chain_status | suspend_count | updated_at          |
+========+============+=======+==============+===============+=====================+
| 396391 | sample_exp | 2     | finished     | 0             | 2021-09-22 15:29:13 |
| 396388 | sample_exp | 1     | finished     | 0             | 2021-09-22 14:31:16 |
+--------+------------+-------+--------------+---------------+---------------------+
```

通过 `hai-cli logs` 查看任务日志。输出由“任务信息 + 任务日志”两部分构成。前半部分与 `hai-cli status` 相同，后半部分为用户任务输出的日志，在每条日志之前，会打印这条日志的时间戳。
```shell
# 持续打印 sample_exp rank 1 的日志：（rank 从 0 开始）
hai-cli logs -f sample_exp --rank 0
# 加上 -f 参数后，会持续追加打印日志，并且 tab 的自动补全只会补全运行中的任务
# 另外，大部分 hai-cli 命令均有 tab 自动补全的功能
# <experiment> 为 experiment_name 时，会选择同名任务中最后一个，如果想查看历史任务的 log，需要使用 experiment_id
# 输出
==================== experiment ====================
+---------+----- ------+-------+--------------+---------------+---------------------+
| id      | nb_name    | nodes | chain_status | suspend_count | created_at          |
+=========+============+=======+==============+===============+=====================+
| 396391  | sample_exp | 2     | finished     | 0             | 2021-09-22 15:34:45 |
+---------+-------- ---+-------+--------------+---------------+---------------------+
==================== jobs ====================
+------+---------+-------------+---------------------+
| rank | status  | node        | started_at          |
+======+=========+=============+=====================+
| 0    | failed  | hfai-rank-0 | 2021-09-22T15:34:45 |
| 1    | stopped | hfai-rank-1 | 2021-09-22T15:34:45 |
+------+---------+-------------+---------------------+
====================  fetching  log on rank 0... ====================
[2021-09-22 15:34:53.282729] [训练前检查] 检查[MARSV2] cpu memory 24.0 < 100G and total gpu memory 0 < 100M 通过
...
```