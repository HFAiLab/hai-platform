# 安装与设置

本文将介绍如何部署 HAI Platform，构建可以支持大规模加速卡并可以分时调度的深度学习训练集群。

## 基础设施
- 一个部署 [Kubernetes](https://kubernetes.io/) 的**算力集群**，包括管理节点（k8s master）、算力节点、服务节点（用于服务部署、数据库、监控等）
- 一个或多个**存储集群**，让所有算力节点可访问的**文件系统**，如 `nfs`, `ceph`, `weka`, [`3FS`](https://www.high-flyer.cn/blog/3fs/) 等。存储信息包括：
    - 用户的运行代码
    - 用户的训练数据
    - 代码运行输出的日志
    - 部署需要的 k8s 配置文件
    - ...
- 算力集群与存储集群之间所有节点通过**高速网络互联**，建议使用 RDMA

## 容器化部署

按照如下几步流程您可以构建部署起 HAI Platform：

1. 获取平台镜像 `registry.cn-hangzhou.aliyuncs.com/hfai/hai-platform:latest`；
  
2. 安装命令行工具 `hai-up`, `hai-cli`

    ```
    $ pip3 install hai --extra-index-url https://pypi.hfai.high-flyer.cn/simple --trusted-host pypi.hfai.high-flyer.cn
    ```

3. 生成配置文件

    ```
    bash hai-up.sh dryrun --provider k8s
    ```

    配置文件可以按照您的需求进行调整，各配置项详细内容参见[这里](https://github.com/HFAiLab/hai-platform/blob/main/README.md#%E9%99%84%E5%BD%95%E9%85%8D%E7%BD%AE%E8%AF%B4%E6%98%8E)；

5. 部署并启动服务

    ```
    bash hai-up.sh up --provider k8s
    ```

6. 使用 `hai-cli`　初始化和提交任务。

<br />更详细的配置指引可以阅读[ HAI Platform 开源仓库](https://github.com/HFAiLab/hai-platform/blob/main/README.md)。