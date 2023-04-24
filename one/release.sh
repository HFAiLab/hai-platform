#!/bin/bash
set -e

RELEASE_VERSION=$(git rev-parse --short HEAD)
IMAGE_REPO="${IMAGE_REPO:-registry.cn-hangzhou.aliyuncs.com/hfai/hai-platform}"
BUILD_DIR="build"
rm -rf ${BUILD_DIR} && mkdir -p ${BUILD_DIR}
echo "STEP: prepare build directory"
ls -A | grep -vE "^${BUILD_DIR}$|\.git*|__pycache__" | awk '{print $1}' | xargs -I {} sh -c "cp -r {} ${BUILD_DIR}/"
cd ${BUILD_DIR}

echo "STEP: build hai-platform ${RELEASE_VERSION}"
if [[ ${BUILD_TRAIN_IMAGE} == "1" ]]; then
cat >> Dockerfile << EOF
#### create haienv 202207 ####
RUN ["/bin/bash", "-c", "export HAIENV_PATH=/hf_shared/hfai_envs/platform && \
    mkdir -p /hf_shared/hfai_envs/platform && chmod 777 /hf_shared/hfai_envs && \
    echo Y | haienv create hai202207 --no_extend"]

RUN --mount=type=cache,sharing=private,target=/root/.cache/pip --mount=type=bind,source=one/requirements-202207.txt,target=/tmp/requirements-202207.txt \
    ["/bin/bash", "-c", "export HAIENV_PATH=/hf_shared/hfai_envs/platform && \
    source haienv hai202207 && \
    pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple && \
    pip config set install.trusted-host pypi.tuna.tsinghua.edu.cn && \
    pip config set wheel.trusted-host pypi.tuna.tsinghua.edu.cn && \
    echo unset pip cache ttl && \
    sed -i '/def _cache_set(self, cache_url, request, response, body=None, expires_time=None):/a\\\        expires_time=None' \`python -c 'import pip._vendor.cachecontrol.controller as cc; print(cc.__file__)'\` && \
    pip install -r /tmp/requirements-202207.txt --no-warn-conflicts --no-deps"]
EOF

sed -i 's/ ubuntu:20.04/ nvcr.io\/nvidia\/cuda:11.3.0-devel-ubuntu20.04/' Dockerfile
RELEASE_VERSION=${RELEASE_VERSION}-202207
fi

DOCKER_BUILDKIT=1 docker build . -t ${IMAGE_REPO}:${RELEASE_VERSION} --build-arg HAI_VERSION=${RELEASE_VERSION} -f Dockerfile --progress plain
echo "build hai success:"
echo "  hai platform image: ${IMAGE_REPO}:${RELEASE_VERSION}"

ID=`docker run -it -d --rm ${IMAGE_REPO}:${RELEASE_VERSION} sleep 60` > /dev/null
WHL_PATH=`docker exec ${ID} find /high-flyer/code/multi_gpu_runner_server/ -maxdepth 1 -name hai*.whl` > /dev/null
WHL_PATH_ARR=(${WHL_PATH})
for p in ${WHL_PATH_ARR[@]}; do docker cp ${ID}:${p} . > /dev/null; done
docker kill ${ID} > /dev/null
WHL_PATH_ARR=(`ls $(pwd)/hai*.whl`)
echo "  hai-cli whl:"
for f in ${WHL_PATH_ARR[@]}; do echo "    $f"; done

echo "push hai-platform image:"
docker push ${IMAGE_REPO}:${RELEASE_VERSION}
