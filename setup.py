#!/usr/bin/env python
import os
"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements_file_lst = ["requirements.txt"]
install_req = []
directory = os.getcwd()

for file in requirements_file_lst:
    with open(os.path.join(directory, file), "r", encoding="utf-8") as fr:
        req_texts = [n.strip() for n in fr.readlines()]
        req_texts = [t for t in req_texts if len(t) > 0]
        install_req.extend(req_texts)

test_requirements = ['pytest>=3', ]

setup(
    author="Dexter Chan",
    author_email='dexterchan@gmail.com',
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    description="Testing nats functionality",
    entry_points={
        'console_scripts': [
            'nats_lab=nats_lab.cli:main',
        ],
    },
    install_requires=install_req,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='nats_lab',
    name='nats_lab',
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/dexterchan/nats_lab',
    version='0.1.11',
    zip_safe=False,
)
