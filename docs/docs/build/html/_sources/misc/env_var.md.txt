# 环境变量

在 HAI Platform 上的任务、节点、训练等信息会由系统分配保存在环境变量中，用户可以在代码中使用这些环境变量来获得对任务的控制

## 模型运行时信息

* `WORLD_SIZE`
  * 类型: int string，如 `'1'`
  * 有多少台机器一起训练
* `RANK`
  * 类型: int string，如 `'0'` 
  * 集群中的第几台机器
* `MASTER_IP`
  * 类型: string，如 `'192.168.10.100'`
  * 集群的第一台机器的 IP 地址
* `MASTER_PORT`
  * 类型: int string 如 `'29510'`
  * 集群第一台机器暴露的端口

## 用户信息

* `MARSV2_UID`
  * 类型: int string，如 `'10001'`
  * 运行用户的 uid
* `MARSV2_USER`
  * 类型: string
  * 运行用户名
* `MARSV2_USER_TOKEN`
  * 类型: string
  * 用户提交任务的 token
* `MARSV2_USER_ROLE`
  * 类型: enum string, `'INTERNAL'`, `'EXTERNAL'`
  * 用户角色：内部用户 `'INTERNAL'`、外部用户`'EXTERNAL'`

## 任务信息
* `MARSV2_NB_NAME`
  * 类型: string
  * 任务名
* `MARSV2_TASK_TYPE`
  * 类型: enum string, `'training'`, `'jupyter'`, `'validation'`
  * 任务类型
* `MARSV2_TASK_ID`
  * 类型: int string, 如 `'55'`
  * 任务 ID
* `MARSV2_WHOLE_LIFE_STATE`
  * 类型: int string, 如 `'0'`
  * 任务处于什么状态

## 日志目录
* `MARSV2_LOG_DIR`
  * 类型: string，如 `/marsv2/log/55`
  * 任务 stdout 的日志目录
* `MARSV2_LOG_FILE_PATH`
  * 类型: string，如 `/marsv2/log/55/jd-a1005-dl#0`
  * 任务 stdout 的日志文件地址
* `MARSV2_DEBUG_LOG_FILE_PATH`
  * 类型: string，如 `/marsv2/log/55/debug_jd-a1005-dl#0`
  * 调度内部 debug 的日志位置

## 节点信息
* `MARSV2_RANK`
  * 类型: int string, 如 `'8'`
  * 节点处于调度第几个 rank
* `MARSV2_NODE_NAME`
  * 类型: string, 如 `'jd-a0101-dl'`
  * 节点名字
