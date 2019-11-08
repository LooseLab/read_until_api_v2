#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""
from setuptools import setup, find_packages

PKG = "read_until_api_v2"

__version__ = ""  # Define empty version that is overwritten below
exec(open("{}/_version.py".format(PKG)).read())

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = []

setup_requirements = []

test_requirements = []

setup(
    author="Alex Payne",
    author_email='alexander.payne@nottingham.ac.uk',
    python_requires='>=3.5, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, !=2.*',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Python3 implementation of the read_until_api",
    install_requires=requirements,
    long_description=readme,
    include_package_data=True,
    keywords='read_until_api_v2',
    name='read_until_api_v2',
    packages=find_packages(include=['read_until_api_v2', 'read_until_api_v2.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/alexomics/read_until_api_v2',
    version=__version__,
    zip_safe=False,
)
