#!/usr/bin/env python
# Installs vsqs.

import os
import sys
from distutils.core import setup


def long_description():
    """Get the long description from the README"""
    return open(os.path.join(sys.path[0], 'README.rst')).read()

setup(
    author='Erik van Zijst',
    author_email='erik.van.zijst@gmail.com',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Clustering',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Filesystems',
    ],
    description="A very simple message queuing system inspired on Amazon's SQS.",
    download_url='https://github.com/erikvanzijst/vsqs/archive/0.3.tar.gz',
    install_requires=['watchdog>=0.8.2'],
    keywords='mq broker message queue',
    license='MIT',
    long_description=long_description(),
    name='vsqs',
    packages=['vsqs'],
    scripts=['scripts/vsqspump', 'scripts/vsqssink'],
    url='https://github.com/erikvanzijst/vsqs',
    version='0.3',
)
