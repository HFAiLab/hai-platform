
set -e

BUILD_DIR="build"
rm -rf ${BUILD_DIR} && mkdir -p ${BUILD_DIR}
ls -A | grep -vE "^${BUILD_DIR}$|\.git*|__pycache__" | awk '{print $1}' | xargs -I {} sh -c "cp -r {} ${BUILD_DIR}/"
cp -r ../../client/api/api_utils.py ${BUILD_DIR}/haiworkspace/client
cp -r ../../client/api/api_config.py ${BUILD_DIR}/haiworkspace/client
cp -r ../../conf/utils.py ${BUILD_DIR}/haiworkspace/client
cp -r ../../cloud_storage/provider/ ${BUILD_DIR}/haiworkspace/client/provider

pushd ${BUILD_DIR}
  python3 setup.py bdist_wheel
  cp dist/haiworkspace*.whl /tmp
popd

echo "build haiworkspace success: `ls /tmp/haiworkspace*.whl`"

# rm -rf build
