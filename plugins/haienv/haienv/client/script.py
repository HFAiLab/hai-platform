ACTIVATE = '''
#!/usr/bin/env bash


deactivate () {
    # unset
    conda deactivate 2> /dev/null || true
    conda deactivate 2> /dev/null || true
    unset CMD
    unset CMD_LEN
    unset VENV
    unset PY_MAJ
    unset VENV_ROOT
    unset VENV_PATH

    if ! [[ -z "${_OLD_VENV_PATH+_}" ]]; then
        PATH="$_OLD_VENV_PATH"
        export PATH
        unset _OLD_VENV_PATH
    fi

    if ! [[ -z "${_OLD_HF_ENV_NAME+_}" ]]; then
        HF_ENV_NAME="$_OLD_HF_ENV_NAME"
        export HF_ENV_NAME
        unset _OLD_HF_ENV_NAME
    fi

    if ! [[ -z "${_OLD_HF_ENV_OWNER+_}" ]]; then
        HF_ENV_OWNER="$_OLD_HF_ENV_OWNER"
        export HF_ENV_OWNER
        unset _OLD_HF_ENV_OWNER
    fi

    if ! [[ -z "${_OLD_PYTHON_PATH+_}" ]]; then
        PYTHONPATH="$_OLD_PYTHON_PATH"
        export PYTHONPATH
        unset _OLD_PYTHON_PATH
    fi

    if ! [[ -z "${_OLD_PS1+_}" ]]; then
        PS1="$_OLD_PS1"
        export PS1
        unset _OLD_PS1
    fi

    if ! [[ -z "${_OLD_PIP_CONFIG_FILE+_}" ]]; then
        PIP_CONFIG_FILE="$_OLD_PIP_CONFIG_FILE"
        export PIP_CONFIG_FILE
        if [ ! ${PIP_CONFIG_FILE} ]; then
            unset PIP_CONFIG_FILE
        fi
        unset _OLD_PIP_CONFIG_FILE
    fi

    if ! [[ -z "${_OLD_PYTHONUSERBASE+_}" ]]; then
        PYTHONUSERBASE="$_OLD_PYTHONUSERBASE"
        export PYTHONUSERBASE
        if [ ! ${PYTHONUSERBASE} ]; then
            unset PYTHONUSERBASE
        fi
        unset _OLD_PYTHONUSERBASE
    fi

    if [[ ! "${1-}" = "nondestructive" ]]; then
    # Self destruct!
        unset -f deactivate
    fi
    
    if [ -f ${hfai_restore_environment_file} ]; then
        source ${hfai_restore_environment_file}
        rm -rf ${hfai_restore_environment_file}
    fi
    
    unset HFAI_ENV_EXTEND_PATH
    unset HFAI_ENV_EXTEND_BIN_PATH
    unset HFAI_ENV_EXTEND_ENVIRONMENT
}

mkdir -m 777 -p ~/.haienv
hfai_old_environment_file=~/.haienv/hfai_env_old_environment
hfai_extra_environment_file=~/.haienv/hfai_env_extra_environment
hfai_restore_environment_file=~/.haienv/hfai_env_restore_environment
deactivate nondestructive

export HFAI_ENV_EXTEND_PATH=${NEW_HFAI_ENV_EXTEND_PATH}
export HFAI_ENV_EXTEND_BIN_PATH=${NEW_HFAI_ENV_EXTEND_BIN_PATH}
export HFAI_ENV_EXTEND_ENVIRONMENT=${NEW_HFAI_ENV_EXTEND_ENVIRONMENT}
unset NEW_HFAI_ENV_EXTEND_PATH
unset NEW_HFAI_ENV_EXTEND_BIN_PATH
unset NEW_HFAI_ENV_EXTEND_ENVIRONMENT
_OLD_VENV_PATH="${PATH}"
__EXTEND_HF_ENV__
export PATH

_OLD_PYTHON_PATH="${PYTHONPATH}"
PYTHONPATH="${HFAI_ENV_EXTEND_PATH}"

PYTHONPATH=".:${PYTHONPATH}"
export PYTHONPATH

# HF_ENV_NAME 和 HF_ENV_OWNER 是上传到萤火集群时用的
_OLD_HF_ENV_NAME="${HF_ENV_NAME}"
HF_ENV_NAME=__HF_ENV_NAME__
export HF_ENV_NAME

_OLD_HF_ENV_OWNER="${HF_ENV_OWNER}"
HF_ENV_OWNER=__HF_ENV_OWNER__
export HF_ENV_OWNER

_OLD_PS1="${PS1-}"
PS1="__NAME__ ${PS1-}"
export PS1

_OLD_PIP_CONFIG_FILE="${PIP_CONFIG_FILE}"
PIP_CONFIG_FILE="__PIP_PATH__"
export PIP_CONFIG_FILE

_OLD_PYTHONUSERBASE="${PYTHONUSERBASE}"
PYTHONUSERBASE="__PATH__"
export PYTHONUSERBASE

if [[ __IS_HF_ENV__ != 1 ]]; then
    conda config --set changeps1 false
    if (($?)); then
        echo "conda有问题，请检查conda或是换个目录重试"
        deactivate nondestructive
        return -1
    fi
    __conda_setup="$('conda' 'shell.bash' 'hook' 2> /dev/null)"
    eval "$__conda_setup"
    unset __conda_setup
    activate_dir=${BASH_SOURCE[0]:-${(%):-%x}}
    basepath=$(cd `dirname ${activate_dir}`; pwd)
    conda activate ${basepath}
fi

export PATH=${HFAI_ENV_EXTEND_BIN_PATH}${PATH}

extend_environment_array=(${HFAI_ENV_EXTEND_ENVIRONMENT//:/ })
for var in ${extend_environment_array[@]}; do
  key=`echo ${var/=/ } | awk '{print $1}'`
  value=`echo ${var/=/ } | awk '{print $2}'`
  echo "export ${key}=${value}" >> ${hfai_extra_environment_file}
  echo "_OLD_${key}=\${${key}}" >> ${hfai_old_environment_file}
  
  echo "${key}=\${_OLD_${key}}" >> ${hfai_restore_environment_file}
  echo "export ${key}" >> ${hfai_restore_environment_file}
  echo "if [ ! \${${key}} ]; then" >> ${hfai_restore_environment_file}
  echo "    unset ${key}" >> ${hfai_restore_environment_file}
  echo "fi" >> ${hfai_restore_environment_file}
  echo "unset _OLD_${key}" >> ${hfai_restore_environment_file}
done

if [ -f ${hfai_old_environment_file} ]; then
    source ${hfai_old_environment_file}
    rm -rf ${hfai_old_environment_file}
fi

if [ -f ${hfai_extra_environment_file} ]; then
    source ${hfai_extra_environment_file}
    rm -rf ${hfai_extra_environment_file}
fi

unset basepath
echo "user haienv __NAME__ loaded"
'''
