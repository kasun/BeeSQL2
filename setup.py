#!/usr/bin/env python

#from distutils.core import setup
from setuptools import setup

requires = ['PyMySQL']
setup(name='BeeSQL',
    version='0.1',
    description='Pythonic SQL library',
    author='Kasun Herath',
    author_email='kasunh01@gmail.com',
    install_requires=requires,
    py_modules=['beesql'],
    )
