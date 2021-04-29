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
    'ipython==7.22.0',
    'pyspark==3.1.1',
    'jupyterhub==1.3.0',
    'oauthenticator==14.0.0',
    'requests==2.25.1',
    'requests-cache==0.5.2',
    'responses==0.13.2',
    'ipykernel==5.5.3',
    'notebook==6.3.0',
    'tornado==6.1',
    'gcsfs==0.6.2',
    'pyarrow==3.0.0',
    'pandas==1.2.4',
    'google-auth==1.28.1',
    'google-auth-oauthlib==0.4.4',
    'ipywidgets==7.6.3'
]

setup(
    name="ssb-ipython-kernels",
    version="0.2.27",
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
        'Programming Language :: Python :: 3.8',
    ],
)
