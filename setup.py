try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='atsd_finder',
    install_requires=['requests'],
    packages=['atsd_finder']
)