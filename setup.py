import io
import os
import re

from setuptools import find_packages
from setuptools import setup


def read(filename):
    filename = os.path.join(os.path.dirname(__file__), filename)
    text_type = type(u"")
    with io.open(filename, mode="r", encoding='utf-8') as fd:
        return re.sub(text_type(r':[a-z]+:`~?(.*?)`'), text_type(r'``\1``'), fd.read())


DEPENDENCIES = [
    'ipython>=4.0.2'
    'pyspark'
    'jupyterhub'
    'oauthenticator'
    'jwt'
    'requests'
    'responses'
    'ipykernel>=4.2.2'
    'notebook>=4.2'
    'tornado>=4'
    'gcsfs'
    'pyarrow>=0.17.*'
    'pandas'
    'google-auth>=1.2'
    'google-auth-oauthlib'
    'ipywidgets'
]

setup(
    name="ssb-ipython-kernels",
    version="0.1.5",
    url="https://github.com/statisticsnorway/dapla-ipython-kernels",
    license='MIT',

    author="Statistics Norway",
    author_email="bjorn.skaar@ssb.no",

    description="Jupyter kernels for working with dapla services",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",

    packages=find_packages(exclude=('tests', 'examples',)),

    install_requires=DEPENDENCIES,

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
