# 在 multi_gpu_runner_server 运行，会构建下面的目录，然后把他们加到 PYTHONPATH 中
# 在 windows 下会是一个 空目录，只是为了 pycharm 的引用方便，所以需要人工刷新
# .hfai
#    ├── base_model -> ../base_model/
#    ├── client -> ../client
#    ├── __init__.py -> ../client_init.py
#    └── conf
#        ├── __init__.py
#        └── flags.py -> ../../conf/flags.py
#
#4 directories, 3 files
set +e

current_path=`pwd`
if [ `basename ${current_path}` != "multi_gpu_runner_server" ]; then
  echo "请在 multi_gpu_runner_server 运行"
  exit 1
fi

rm -rf hfai
mkdir -p hfai
pushd hfai
  ln -s ${current_path}/base_model/
  ln -s ${current_path}/client
  ln -s ${current_path}/client_init.py __init__.py
  echo from hfai import client >> __init__.py
  mkdir -p conf
  pushd conf
    touch __init__.py
    ln -s ${current_path}/conf/flags
    ln -s ${current_path}/conf/utils.py
  popd
popd

export PYTHONPATH=${current_path}:${PYTHONPATH}
#set -e