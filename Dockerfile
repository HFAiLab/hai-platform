# syntax = docker/dockerfile:1
FROM ubuntu:20.04 as base

ARG HAI_VERSION

# apt install
RUN --mount=type=cache,sharing=private,target=/var/cache/apt \
  apt-get update && DEBIAN_FRONTEND=noninteractive TZ=Asia/Shanghai apt-get -y install tzdata && \
  apt-get install -y python3.8 python3-pip tzdata libcurl4-openssl-dev libssl-dev net-tools \
    apache2-utils infiniband-diags g++ libpq-dev python3-dev openssh-server sudo curl python-numpy \
    gcc automake autoconf libtool make gdb strace moreutils dnsutils pv rsync vim less git libgomp1 lsb-release jq \
    libcap-dev libcap2-bin libnuma-dev numactl libopenmpi-dev \
    haproxy redis postgresql attr

RUN ln -sf /usr/bin/python3.8 /usr/bin/python

# binary files
RUN curl -Lo /usr/local/bin/kubectl https://yinghuoai-public.oss-cn-hangzhou.aliyuncs.com/build_deps/kubectl-v1.20.15 && \
  curl -Lo /usr/local/bin/decode-protobuf-camel https://yinghuoai-public.oss-cn-hangzhou.aliyuncs.com/build_deps/decode-protobuf-camel && \
  chmod +x /usr/local/bin/kubectl /usr/local/bin/decode-protobuf-camel

# install conda
RUN curl -Lo /tmp/Miniconda3-4.12.0-Linux-x86_64.sh https://yinghuoai-public.oss-cn-hangzhou.aliyuncs.com/build_deps/Miniconda3-4.12.0-Linux-x86_64.sh && \
  /bin/bash /tmp/Miniconda3-4.12.0-Linux-x86_64.sh -b -p /usr/local/conda && \
  ln -sf /usr/local/conda/bin/conda /usr/local/bin/conda && \
  rm /tmp/Miniconda3-4.12.0-Linux-x86_64.sh

# install fountain and ambient
RUN mkdir -p /marsv2/scripts && \
  curl https://yinghuoai-public.oss-cn-hangzhou.aliyuncs.com/build_deps/ambient.tar.gz | tar zxvf - -C /marsv2/scripts && \
  curl https://yinghuoai-public.oss-cn-hangzhou.aliyuncs.com/build_deps/fountain.tar.gz | tar zxvf - -C /marsv2/scripts

# system config
RUN mkdir -p /run/sshd && \
  echo SHELL=/bin/bash >> /etc/default/useradd && \
  echo "ALL ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers && \
  ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' > /etc/timezone && \
  echo "* soft nproc 655350\n* hard nproc 655350\n* soft nofile 655350\n* hard nofile 655350\n* soft memlock unlimited\n* hard memlock unlimited\n" >> /etc/security/limits.conf && \
  echo "[supervisorctl]\nserverurl=unix:///tmp/supervisor.sock" > /etc/supervisord.conf

# 安装 setuptools_scm, 升级pip以支持cache
RUN pip install "setuptools_scm>=6.3.2" --index-url=https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host=pypi.tuna.tsinghua.edu.cn && \
  pip install "pip==23.0" --index-url=https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host=pypi.tuna.tsinghua.edu.cn

RUN --mount=type=cache,target=/root/.cache/pip \
  --mount=type=bind,source=requirements.txt,target=/tmp/requirements.txt \
  --mount=type=bind,source=client/requirements_38.txt,target=/tmp/requirements_38.txt \
  pip install -r /tmp/requirements.txt \
  -r /tmp/requirements_38.txt \
  --index-url=https://pypi.tuna.tsinghua.edu.cn/simple \
  --trusted-host=pypi.tuna.tsinghua.edu.cn

# 安装 studio
RUN mkdir -p /marsv2/scripts/studio && \
  curl https://yinghuoai-public.oss-cn-hangzhou.aliyuncs.com/build_deps/hai-studio-linux-x64-0.0.2.tar.gz | tar zxvf - -C /marsv2/scripts/studio

############################################################

FROM base as packaging

# hfai
COPY . /high-flyer/code/multi_gpu_runner_server

# packaging
RUN cd /high-flyer/code/multi_gpu_runner_server && \
  HAI_VERSION=${HAI_VERSION} bash one/build_cli.sh

############################################################

FROM base

ENV HAIENV_PATH=/hf_shared/hfai_envs/platform HAI_VERSION=${HAI_VERSION}

# install hai-cli, hai plugin
RUN --mount=type=bind,from=packaging,source=/tmp,target=/tmp \
  pip install /tmp/hai*.whl --index-url=https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host=pypi.tuna.tsinghua.edu.cn

COPY --from=packaging /high-flyer/code/multi_gpu_runner_server /tmp/hai*.whl /high-flyer/code/multi_gpu_runner_server
