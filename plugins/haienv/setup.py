import os
import subprocess
from setuptools import setup, find_packages


cmd = ['git', 'rev-parse', '--short', 'HEAD']
try:
    rev = '+' + subprocess.check_output(cmd).decode('ascii').rstrip()
except:
    rev = '+' + os.environ.get('HAI_VERSION', '')
version = "1.4.1" + rev

requires = []

with open('requirements.txt') as r:
    req_txt = r.read()
    requires += req_txt.strip().split('\n')

setup(
    name='haienv',
    version=version,
    description='Highflyer AI Env',
    author='HFAiLab',
    license='MIT',
    url='https://github.com/HFAiLab',
    python_requires='>=3.8',
    install_requires=requires,
    packages=find_packages(),
    scripts=['haienv/haienv']
)
