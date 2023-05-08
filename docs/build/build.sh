# sphinx-quickstart
rm -rf ../!"(build)"

cd docs
make clean && make html

mv build/html/* ../../
rm -rf build

cd ../../
touch .nojekyll
