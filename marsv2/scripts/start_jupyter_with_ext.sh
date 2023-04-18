export JUPYTER_LAB_USER=${MARSV2_USER}
export USER_TOKEN=${MARSV2_USER_TOKEN}
export MARS_URL=${MARSV2_SERVER}

export JUPYTERLAB_SETTINGS_DIR=${JUPYTER_DIR}/jupyter_config/lab/user-settings
export JUPYTERLAB_WORKSPACES_DIR=${JUPYTER_DIR}/jupyter_config/lab/workspaces
export JUPYTER_CONFIG_DIR=${JUPYTER_DIR}/jupyter_config
export NOTEBOOK_DIR=${JUPYTER_DIR}/notebooks
export PYTHONPATH=/marsv2/scripts:${PYTHONPATH}

export SHELL=/bin/bash
export LANG=C.UTF-8
export LC_ALL=C.UTF-8

export JUPYTERHUB_SERVER_NAME="fake"
export JUPYTERHUB_USER_ROLE=${MARSV2_USER_ROLE}
export JUPYTER_LAB_AVAILABLE_PATH_SIGNATURE=QS0tnxUAqlhA+qRw9N8lu5B4INpzc++NwLwJqxca38niN6JpAEtzvUSyQGbRmqUTBYFR3szPCRLkzK45XL+xGh9gsrA/uTotxAl3QD4BGWdYlVWg31CoaJkOwGBp5mwgLvMp1GP3d9KDaxGMfWN8vBzxCrH2H0Q74GziFoZAF8M=

if which hai-cli > /dev/null; then
  echo "use hai-cli init"
  hai-cli init ${USER_TOKEN} --url ${MARS_URL}
else
  echo "use hfai init"
  if [ ${USER_BIN_PYTHON}x == "/usr/bin/python3.8"x ]; then
    source hf_env38 202111
  else
    if [ ${NV_CUDA_COMPAT_PACKAGE}x == "cuda-compat-11-3"x  ]; then
      source hf_env3 202111
    else
      source hf_env3 202105
    fi
  fi
  # hfai
  hfai init ${USER_TOKEN} --url ${MARS_URL}
  python -c "from hfai.client.api import set_swap_memory; set_swap_memory(10000)"
fi

# jupyterlab
mkdir -p ${NOTEBOOK_DIR}
if [ ! -x "${JUPYTERLAB_SETTINGS_DIR}/HF_AiLab_ext/config.jupyterlab-settings" ]; then
  mkdir -p ${JUPYTERLAB_SETTINGS_DIR}/HF_AiLab_ext
  echo {\"token\": \"${USER_TOKEN}\"} > ${JUPYTERLAB_SETTINGS_DIR}/HF_AiLab_ext/config.jupyterlab-settings
fi
mkdir -p ${JUPYTERLAB_WORKSPACES_DIR}

# 插件 server 端挂载路径
export PYTHONPATH=$PYTHONPATH:/jupyter_ext/server:/marsv2/scripts:/high-flyer/code/multi_gpu_runner_server

echo "[jupyter start]jupyterhub-singleuser start, PYTHONPATH: $PYTHONPATH"

cd ~

# pip list | grep traitlets |  awk '{print $2}' 耗时较长，弃用
traitlets_version=`python3 -c "import traitlets; print(traitlets.__version__)"`

echo "[jupyter start]current traitlets_version:$traitlets_version"

function version_gt() { test "$(echo "$@" | tr " " "\n" | sort -V | head -n 1)" != "$1"; }

# 内部用户开启 Jupyter 删除文件放入回收站(.Trash 文件夹)，外部用户不开：
DELETE_TO_TRASH=`[ $MARSV2_USER_ROLE == 'internal' ] && echo 'True' || echo 'False'`

# HINT: traitlet 的 4 和 5 版本不兼容，需要区分写法，这里由于 Shell 语法无法完全通过变量兼容两种写法的问题，我们分开维护
if version_gt $traitlets_version 5.0.0; then
  echo "$traitlets_version is greater than 5.0.0"
  jupyter-lab --ip=0.0.0.0 --port ${JUPYTER_PORT} --LabApp.extra_labextensions_path=/jupyter_ext/HF_AiLab_ext/empty --LabApp.extra_labextensions_path=/jupyter_ext/client --LabApp.default_url=/lab --ServerApp.notebook_dir=${NOTEBOOK_DIR} --ServerApp.jpserver_extensions="{'HF_AiLab_ext':True}" --ServerApp.token=${USER_TOKEN} --ServerApp.base_url=/${MARSV2_USER}/${MARSV2_NB_NAME} --NotebookNotary.db_file=':memory:' --ServerApp.kernel_spec_manager_class=hf_kernel_spec_manager.HFKernelSpecManager --ServerApp.kernel_manager_class=hf_kernel_spec_manager.HFKernelManager --FileContentsManager.delete_to_trash=${DELETE_TO_TRASH} --ServerApp.login_handler_class=hf_login_handler.HFLoginHandler
else
  echo "$traitlets_version is not greater than 5.0.0"
  jupyter-lab --ip=0.0.0.0 --port ${JUPYTER_PORT} --LabApp.extra_labextensions_path='["/jupyter_ext/HF_AiLab_ext/empty", "/jupyter_ext/client"]' --LabApp.default_url=/lab --ServerApp.notebook_dir=${NOTEBOOK_DIR} --ServerApp.jpserver_extensions="{'HF_AiLab_ext':True}" --ServerApp.token=${USER_TOKEN} --ServerApp.base_url=/${MARSV2_USER}/${MARSV2_NB_NAME} --NotebookNotary.db_file=':memory:' --ServerApp.kernel_spec_manager_class=hf_kernel_spec_manager.HFKernelSpecManager --ServerApp.kernel_manager_class=hf_kernel_spec_manager.HFKernelManager --FileContentsManager.delete_to_trash=${DELETE_TO_TRASH} --ServerApp.login_handler_class=hf_login_handler.HFLoginHandler
fi


