#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
setup.py
A module that installs the wmrc skid as a module
"""
from pathlib import Path

from setuptools import find_packages, setup

#: Load version from source file
version = {}
version_file = Path(__file__).parent / "src" / "wmrc" / "version.py"
exec(version_file.read_text(), version)


setup(
    name="wmrc-skid",
    version=version["__version__"],
    license="MIT",
    long_description=(Path(__file__).parent / "README.md").read_text(),
    long_description_content_type="text/markdown",
    author="Jacob Adams",
    author_email="jdadms@utah.gov",
    url="https://github.com/agrc/wmrc-skid",
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    zip_safe=True,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Utilities",
    ],
    project_urls={
        "Issue Tracker": "https://github.com/agrc/wmrc-skid/issues",
    },
    keywords=["gis"],
    install_requires=[
        "ugrc-palletjack>=4.1,<5.1",
        "agrc-supervisor==3.0.3",
        "google-cloud-storage>=2.16,<2.19",
    ],
    extras_require={
        "tests": [
            "pytest-cov>=3,<6",
            "pytest-instafail==0.5.*",
            "pytest-mock==3.*",
            "pytest-ruff==0.*",
            "pytest-watch==4.*",
            "pytest>=6,<9",
            "black>=23.3,<24.9",
            "ruff==0.*",
            "functions-framework>=3.4,<3.9",
        ]
    },
    setup_requires=[
        "pytest-runner",
    ],
    entry_points={
        "console_scripts": [
            "wmrc = wmrc.main:process",
        ]
    },
)
