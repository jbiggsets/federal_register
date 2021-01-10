#!/usr/bin/env python
from setuptools import find_packages, setup


def readme():
    with open('README.rst') as f:
        return f.read()


def requirements():
    req_path = 'requirements.txt'
    with open(req_path) as f:
        reqs = f.read().splitlines()
    return reqs


setup(name='federal_register',
      version=0.1,
      description='Pull data from the federal register, and do data science',
      long_description=readme(),
      keywords=['federal register', 'api', 'nlp'],
      maintainer='Jeremy Biggs',
      maintainer_email='jeremy.m.biggs@gmail.com',
      license='Apache 2',
      packages=find_packages(),
      include_package_data=True,
      entry_points={'console_scripts':
                    ['federal_client = federal_register.federal_register:main']
                    },
      install_requires=requirements(),
      classifiers=['Intended Audience :: Science/Research',
                   'Intended Audience :: Developers',
                   ],
      zip_safe=False)