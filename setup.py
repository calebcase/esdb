import os
from setuptools import setup, find_packages


setup(
    name='esdb',
    version='0.0',
    description='ElasticSearch DB',
    long_description='Python library for working with ElasticSearch.',
    classifiers=[
        'Development Status :: 0 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    author='Caleb Case',
    author_email='calebcase@gmail.com',
    url='https://github.com/calebcase/esdb',
    packages=find_packages(),
    requires=[
        'rawes'
    ],
)
