# sphinx-quickstart
rm -rf ../docs/*
rm -rf docs/build

cd docs
make clean && make html

mv build/html/* ../../docs/
rm -rf build
