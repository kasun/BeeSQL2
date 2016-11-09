from setuptools import setup

requires = ['PyMySQL']
packages = [
    'beesql', 'beesql.query',
]
setup(
    name='BeeSQL',
    version='0.1',
    description='SQL abstraction library for Python',
    author='Kasun Herath',
    author_email='kasunh01@gmail.com',
    install_requires=requires,
    packages=packages,
)
