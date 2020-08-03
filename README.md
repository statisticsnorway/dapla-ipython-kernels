# dapla-ipython-kernels
Python module for use within Jupyter notebooks. It contains kernel extensions for integrating with Apache Spark, 
Google Cloud Storage and custom dapla services.

[![PyPI version](https://img.shields.io/pypi/v/ssb-ipython-kernels.svg)](https://pypi.python.org/pypi/ssb-ipython-kernels/)
[![Status](https://img.shields.io/pypi/status/ssb-ipython-kernels.svg)](https://pypi.python.org/pypi/ssb-ipython-kernels/)
[![License](https://img.shields.io/pypi/l/ssb-ipython-kernels.svg)](https://pypi.python.org/pypi/ssb-ipython-kernels/)

## Getting Started

Install the module from pip:

```bash
# pip
pip install dapla-ipython-kernels
```

Now the module is ready to use with a single import:

```python
import dapla as dp
```

This module is targeted to python kernels in Jupyter, but it may work in any IPython environment. 
It also depends on a number of custom services, e.g. [the custom auth service](dapla/jupyterextensions/authextension.py)

To test, simply create any Pandas dataframe. This can be stored in Google Cloud Storage at a specific path:

```python
import pandas as pd
import dapla as dp

data = {
    'apples': [3, 2, 0, 1], 
    'oranges': [0, 3, 7, 2]
}
# Create pandas DataFrame
purchases = pd.DataFrame(data, index=['June', 'Robert', 'Lily', 'David'])

# Write pandas DataFrame to parquet
dp.write_pandas(purchases, '/testfolder/python/purchases', valuation='INTERNAL', state= 'INPUT')
```

Conversely, parquet files can be read from a path directly into a pandas DataFrame. 
 
```python
import dapla as dp
# Read path into pandas dataframe 
purchases = dp.read_pandas('/testfolder/python/purchases')
```

## Other functions

Since the python module integrates with Google Cloud Storage and custom dapla services, 
some other functions exist as well:

```python
import dapla as dp

# List path by prefix
dp.show('/testfolder/python')
```
| Path  | Timestamp |
| ----------------------------- | ------------- |
| /testfolder/python/purchases  | 1593120298095 |
| /testfolder/python/other  | 1593157667793 |


```python
import dapla as dp

# Show file details
dp.details('/testfolder/python/purchases')
```
| Size  | Name |
| ----- | -------------------------------------- |
| 2908  | 42331105444c9ca0ce049ef6de7160.parquet |


See also the [example notebook](examples/dapla_notebook.ipynb) written for Jupyter.
