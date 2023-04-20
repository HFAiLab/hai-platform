.. hai documentation master file, created by
   sphinx-quickstart on Mon Apr 11 10:51:24 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

欢迎来到 HAI Platform 官方文档
=====================================

.. toctree::
   :maxdepth: 1
   :caption: 开始使用
   :hidden:
   :glob:

   start/hai_intro.rst
   start/install.rst
   
.. toctree::
   :maxdepth: 2
   :caption: 用户须知
   :hidden:
   :titlesonly:
   :glob:

   guide/tutorial.rst
   guide/environment.rst
   guide/studio.rst
   guide/schedule.rst
   

.. toctree::
   :maxdepth: 2
   :caption: CLI 说明
   :hidden:
   :glob:

   cli/user.rst
   cli/ugc.rst
   cli/exec.rst
   cli/task.rst
   cli/cluster.rst


.. toctree::
   :maxdepth: 3
   :caption: API 说明
   :hidden:
   :glob:

   api/hai.rst
   api/client.rst
   api/client_remote.rst


.. toctree::
   :maxdepth: 1
   :caption: 其他
   :hidden:
   :glob:

   misc/env_var
   misc/resources


.. raw:: html

    <embed>
        <div align="center">
        <br/>
         <a href="https://github.com/HFAiLab/hai-platform">
         <img src="./_static/pic/hai_logo.png" alt="Logo" width="450">
         </a>

         <p align="center"><b>大规模高性能深度学习训练平台</b></p>
         <br/>

         <img src="./_static/pic/studio_screenshot3.png" alt="Logo" width="600">
        </div>
        <br />
        <br />
    </embed>


HAI Platform 是幻方 AI 团队开源的 **大规模高性能深度学习训练平台**，其以 **任务级分时调度共享 AI 算力的理念** 将集群零散资源进行整合，统一分配算力，最大化集群整体的利用效率。
平台针对深度学习训练场景下的 **资源管理、任务管理、环境管理、用户管理、数据管理、可视化交互等一般性需求，设计了全流程解决方案，以及灵活的扩展方式**。
用户可以没有顾及地尽情研发模型提交任务，无需关注如何获取、配置计算节点，平台会自动处理任务编排、调度、打断恢复等流程。
便捷的接口设计，简单明了的任务管理界面，HAI Platform 可以让您获得畅快淋漓的模型训练体验。


阅读指南
-----------------

对于绝大部分的初始试用者，您可以从文档 :doc:`《HAI Platform 基本介绍》 <start/hai_intro>` 开始阅读。
它介绍了 HAI Platform 的基本理念、产品功能、环境特点等信息，帮助您快速了解 HAI Platform。


文档索引
----------
* :ref:`genindex`