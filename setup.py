
from __future__ import absolute_import
from setuptools import setup

# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# README as the long description
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

REQUIREMENTS = [i.strip() for i in open('requirements.txt').readlines()]
tests_require = [
    'pytest',
    'mock',
    'spark-testing-base'
]

setup(name='baskervillehall',
      version='1.0.0',
      description='Baskervillehall is a Layer 7 DDoS attacks mitigation system',
      long_description=long_description,
      tests_require=tests_require,
      extras_require={
          'test': tests_require,
      },
      test_suite='pytest.collector',
      install_requires=REQUIREMENTS,
      include_package_data=True,
      package_dir={'': 'src'},
      packages=[
          'baskervillehall',
      ]
)

