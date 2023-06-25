#!/bin/bash
# run as user (will be sourced from `user_scope.sh`)

die() {
  echo "ERROR: $@"
  exit 1
}

mkdir -p ${MARSV2_TASK_WORKSPACE}
pushd ${MARSV2_TASK_WORKSPACE} > /dev/null
  if [ "${MARSV2_GIT_REPO_AS_WORKSPACE}" = "1" ]; then
    # 指定 git repo 运行, clone
    [[ -f ${HOME}/.ssh/id_rsa ]] || die "${HOME}/.ssh 下未配置 rsa key, 无法 clone repo"
    # ssh-keyscan, MARSV2_GIT_REMOTE_REPO is like [ssh://git@git.sample.com:30022/user/repo.git]
    _git_host=$(echo ${MARSV2_GIT_REMOTE_REPO} | awk -F/ '{print $3}')    # "git@git.xxx.com:12345" or "git@git.xxx.com"
    _git_port=$(echo ${_git_host} | awk -F: '{if (NF>1) print "-p "$2}')  # "-p {port}" or empty string
    _git_host=$(echo ${_git_host} | awk -F'[@:]' '{print $2}')            # "git.xxx.com"
    ssh-keyscan ${_git_port} ${_git_host} >> ${HOME}/.ssh/known_hosts 2>/dev/null

    git clone --depth=1 --quiet "${MARSV2_GIT_REMOTE_REPO}" ./            || die "clone 指定的 repo [${MARSV2_GIT_REMOTE_REPO}] 失败"
    if [ -n "MARSV2_GIT_TARGET_REVISION" ]; then
      git fetch --depth=1 --quiet origin "${MARSV2_GIT_TARGET_REVISION}"    || die "fetch 指定的 revision [${MARSV2_GIT_TARGET_REVISION}] 失败"
      git checkout $(git rev-parse FETCH_HEAD) --quiet                      || die "checkout 到指定的 revision [${MARSV2_GIT_TARGET_REVISION}] 失败"
    fi
    echo "Repo [${MARSV2_GIT_REMOTE_REPO} (revision ${MARSV2_GIT_TARGET_REVISION})] has been cloned to [${MARSV2_TASK_WORKSPACE}]"
  fi
  _status=$(git status --porcelain) || die "指定的 workspace 不是合法的 git repo: ${MARSV2_TASK_WORKSPACE}"
  [[ -z "${_status}" ]] || die "指定的 workspace 中有未提交的改动: $(git status)}"

  _remote_url=$(git remote | head -n 1 | xargs git remote get-url)
  if [ "${_remote_url}" != "${MARSV2_GIT_REMOTE_REPO}" ]; then
    die "指定的 workspace 中默认 remote 的 URL 与记录不符 (当前 ${_remote_url} / 记录 ${MARSV2_GIT_REMOTE_REPO})"
  fi

  export MARSV2_CURRENT_GIT_COMMIT_SHA=$(git rev-parse HEAD)
  echo "HEAD commit SHA: ${MARSV2_CURRENT_GIT_COMMIT_SHA}"
  if [[ -n "${MARSV2_GIT_COMMIT_SHA}" && "${MARSV2_CURRENT_GIT_COMMIT_SHA}" != "${MARSV2_GIT_COMMIT_SHA}" ]]; then
    die "workspace 中的当前 HEAD commit 与记录不符 (记录 ${MARSV2_GIT_COMMIT_SHA})"
  fi
  if [ "${MARSV2_GIT_REPO_AS_WORKSPACE}" = "1" ]; then
    python3 /marsv2/scripts/report_git_revision.py || die "记录最新 commit sha 失败"
  fi
popd > /dev/null
