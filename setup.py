#!/usr/bin/env python

import os


# Utility function to read the README file.
# Credit: http://pypi.python.org/pypi/an_example_pypi_project
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

d = dict(name='txRDQ',
         version='0.2.14',
         provides=['txrdq'],
         maintainer='Fluidinfo Inc.',
         maintainer_email='info@fluidinfo.com',
         url='https://launchpad.net/txrdq',
         download_url='https://code.launchpad.net/txrdq',
         packages=['txrdq', 'txrdq.test'],
         keywords=['twisted dispatch job queue'],
         classifiers=[
             'Programming Language :: Python',
             'Framework :: Twisted',
             'Development Status :: 4 - Beta',
             'Intended Audience :: Developers',
             'License :: OSI Approved :: Apache Software License',
             'Operating System :: OS Independent',
             'Topic :: Software Development :: Libraries :: Python Modules',
         ],
         description=('A Twisted class for queueing and dispatching jobs in '
                      'a controlled manner.'),
         long_description=read('README'))

try:
    from setuptools import setup
    _ = setup  # Keeps pyflakes from complaining.
except ImportError:
    from distutils.core import setup
else:
    d['install_requires'] = ['Twisted']

setup(**d)
