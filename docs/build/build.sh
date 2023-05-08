# sphinx-quickstart
rm -rf ../!"(build)"
rm -rf docs/build

cd docs
make clean && make html

mv build/html/* ../../
rm -rf build
