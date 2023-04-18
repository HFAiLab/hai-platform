#!/bin/bash

HAI_PATH="$(dirname "${BASH_SOURCE[0]}")"
HFAI_PATH="$HAI_PATH"/hfai
mkdir -p "$HFAI_PATH"

echo "STEP: build hai-cli  in ${HFAI_PATH}"
bash client/install.sh ${HFAI_PATH}
pushd "$HAI_PATH"
  touch hfai/__init__.py
#  rm -rf build dist/ hai.spec
  echo "__version__='${HAI_VERSION}'" > hfai/version.py
  rm -rf hfai/client/install.sh

  cp hfai/client/hfai hai-cli
  cp hai-up.sh hai-up
  python3 setup.py bdist_wheel
  cp dist/hai*.whl /tmp
  echo "build hai-cli success: `ls /tmp/hai*.whl`"
popd

echo "STEP: build hai plugins"
for i in `ls -d plugins/hai*`; do
  echo "build $i"
  pushd $i; bash install.sh; popd
done
