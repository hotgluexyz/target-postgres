#!/usr/bin/env python

from os import path

from setuptools import setup, find_packages

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='hotglue-target-postgres',
    url='https://github.com/hotgluexyz/target-postgres',
    author='hotglue',
    version="1.0.0",
    description='Singer.io target for loading data into postgres',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_postgres'],
    install_requires=[
        'psycopg2-binary==2.9.11',
        'joblib==1.5.2',
        'ujson==5.11.0',
        'fastjsonschema==2.21.2',
        'singer-python==6.3.0',
        'inflection==0.5.1'
    ],
    extras_require={
        'tests': [
            "pytest==9.0.1"
        ]},
    entry_points='''
      [console_scripts]
      target-postgres-beta=target_postgres:main
      target-postgres=target_postgres:main
    ''',
    packages=find_packages(),
    package_data = {
        'target_postgres': [
            'logging.conf'
            ]
    },
)
