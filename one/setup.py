
import os
import subprocess
from setuptools import setup, find_packages


cmd = ['git', 'rev-parse', '--short', 'HEAD']
try:
    rev = '+' + subprocess.check_output(cmd).decode('ascii').rstrip()
except:
    rev = '+' + os.environ.get('HAI_VERSION', '')
version = '1.0.0' + rev

requires = []
with open('hfai/client/requirements_38.txt') as r:
    req_txt = r.read()
    requires += req_txt.strip().split('\n')

setup(
    name='hai',
    version=version,
    description='Highflyer AI Platform',
    author='HFAiLab',
    license='MIT',
    url='https://github.com/HFAiLab',
    python_requires='>=3.8',
    install_requires=requires,
    packages=find_packages(),
    scripts=['hai-cli', 'hai-up']
)
