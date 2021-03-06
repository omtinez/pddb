#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'pandas', 'bottle'
]

test_requirements = [
    'unittest2'
]

setup(
    name='pddb',
    version='0.3.3',
    description="Prototyping database engine for Python",
    long_description=readme + '\n\n' + history,
    author="Oscar Martinez",
    author_email='omtinez@gmail.com',
    url='https://github.com/omtinez/pddb',
    packages=['pddb'],
    package_dir={'pddb': 'pddb'},
    package_data={'pddb': ['templates/*', 'js/*']},
    include_package_data=True,
    install_requires=requirements,
    license="MIT",
    zip_safe=False,
    keywords='pddb',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
