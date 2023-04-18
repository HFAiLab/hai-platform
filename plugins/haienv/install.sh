
set -e

BUILD_DIR="build"
rm -rf ${BUILD_DIR} && mkdir -p ${BUILD_DIR}
ls -A | grep -vE "^${BUILD_DIR}$|\.git*|__pycache__" | awk '{print $1}' | xargs -I {} sh -c "cp -r {} ${BUILD_DIR}/"

pushd ${BUILD_DIR}
  python3 setup.py bdist_wheel
  cp dist/haienv*.whl /tmp
popd

echo "build haienv success: `ls /tmp/haienv*.whl`"
