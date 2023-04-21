hfai
=======

HAI Platform 提供了任务级分时调度的深度学习训练模式，极大利用智算集群算力。您可以通过 `import hfai` 开启全新训练体验。


.. toctree::
   :maxdepth: 2
   :caption: Contents:


hfai.client
----------------
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

   create_experiment_v2
   bind_hf_except_hook
   get_experiment
   get_experiments
   self_health_check


.. py:currentmodule:: hfai.client.api
.. autosummary::
   :nosignatures:

   Experiment
   

hfai.client.remote
--------------------
.. py:currentmodule:: hfai.client.remote
.. autosummary::
   :nosignatures:

   GlobalSession
   SessionConfig
