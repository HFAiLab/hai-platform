hfai.client
===============

.. toctree::
   :maxdepth: 2
   :caption: Contents:


任务类
-----------

.. py:currentmodule:: hfai.client.api
.. autosummary::
   :nosignatures:
   
   Experiment


创建任务
-----------

.. py:currentmodule:: hfai.client
.. autosummary::
   :nosignatures:
   
   create_experiment_v2
   bind_hf_except_hook
   get_experiment
   get_experiments
   self_health_check


任务管理
-----------

.. py:currentmodule:: hfai.client
.. autosummary::
   :nosignatures:
   
   set_watchdog_time
   set_whole_life_state
   get_whole_life_state
   receive_suspend_command
   go_suspend

   EXP_PRIORITY
   set_priority


.. py:currentmodule:: hfai.client.api
.. autoclass:: Experiment
   :members:

   .. method:: async log_ng(rank, last_seen=None)

      通过rank获取日志，last_seen用于断点续读

   .. method:: async suspend(restart_delay=0)

      打断该任务，restart_delay 暂未实现

   .. method:: async stop()

      结束该任务

.. py:currentmodule:: hfai.client
.. autofunction:: create_experiment_v2
.. autofunction:: bind_hf_except_hook
.. autofunction:: get_experiment
.. autofunction:: get_experiments
.. autofunction:: self_health_check


.. autofunction:: set_watchdog_time
.. autofunction:: set_whole_life_state
.. autofunction:: get_whole_life_state
.. autofunction:: receive_suspend_command
.. autofunction:: go_suspend


.. autoclass:: EXP_PRIORITY
   :members:

.. autofunction:: set_priority