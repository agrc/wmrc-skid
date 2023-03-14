#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
setup.py
A module that installs the wmrc skid as a module
"""
from glob import glob
from os.path import basename, splitext

from setuptools import find_packages, setup

#: Load version from source file
version = {}
with open('src/wmrc/version.py') as fp:
    exec(fp.read(), version)

setup(
    name='wmrc-skid',
    version=version['__version__'],
    license='MIT',
    description='Update the wmrc data from Google Sheets via GCF',
    author='Jacob Adams',
    author_email='jdadms@utah.gov',
    url='https://github.com/agrc/wmrc-skid',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
    include_package_data=True,
    zip_safe=True,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Utilities',
    ],
    project_urls={
        'Issue Tracker': 'https://github.com/agrc/python/issues',
    },
    keywords=['gis'],
    install_requires=[
        'arcgis==2.1.*',
        'ugrc-palletjack==3.*',
        'agrc-supervisor==3.0.*',
    ],
    extras_require={
        'tests': [
            'pylint-quotes~=0.2',
            'pylint~=2.15',
            'pytest-cov~=4.0',
            'pytest-instafail~=0.4',
            'pytest-isort~=3.1',
            'pytest-pylint~=0.19',
            'pytest-watch~=4.2',
            'pytest~=7',
            'yapf~=0.32',
            'pytest-mock==3.10.*',
            'functions-framework',
        ]
    },
    setup_requires=[
        'pytest-runner',
    ],
    entry_points={'console_scripts': [
        'wmrc = wmrc.main:main',
    ]},
)
