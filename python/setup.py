#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""The setup script."""
from setuptools import setup, find_packages

requirements = [
    # TODO: put package requirements here
]
setup(
    name='magellan',
    version='1.0.5',
    description="Magellan",
    long_description="Magellan",
    author="harsha2010",
    url='https://github.com/harsha2010/magellan',
    packages=[package for package in find_packages() if package.startswith('magellan')],
    include_package_data=True,
    install_requires=requirements,
    zip_safe=False,
    keywords='magellan',
)