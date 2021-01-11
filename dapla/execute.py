import os

from dapla.jupyterextensions.authextension import AuthClient, AuthError
from dapla.services.reader import DataSourceReader
from dapla.services.writer import DataSourceWriter
from dapla.services.clients import DatasetDocClient


def read_pandas(path, columns=None):
    """
    Read Parquet data from a path in Dapla.

    Parameters
    ----------
    path : str
        A path that will be mapped to a GCS path in Dapla
    columns : List[str], optional
        Subset of columns to read.

    Returns
    -------
    pyarrow.Table
        Content as a table (of columns).
    """
    reader = DataSourceReader(AuthClient.get_access_token, os.environ['DATA_ACCESS_URL'],  os.environ['CATALOG_URL'])
    try:
        return reader.read(path, columns)
    except AuthError as err:
        err.print_warning()


def show(path):
    reader = DataSourceReader(AuthClient.get_access_token, os.environ['DATA_ACCESS_URL'],  os.environ['CATALOG_URL'])
    try:
        return reader.list(path)
    except AuthError as err:
        err.print_warning()


def details(path):
    reader = DataSourceReader(AuthClient.get_access_token, os.environ['DATA_ACCESS_URL'],  os.environ['CATALOG_URL'])
    try:
        return reader.details(path)
    except AuthError as err:
        err.print_warning()


def write_pandas(df, path, **option_kwargs):
    writer = DataSourceWriter(
        AuthClient.get_access_token,
        os.environ['DATA_ACCESS_URL'],
        os.environ['METADATA_PUBLISHER_URL'],
        os.environ['METADATA_PUBLISHER_PROJECT_ID'],
        os.environ['METADATA_PUBLISHER_TOPIC_NAME'],
        os.environ['CATALOG_URL']
    )
    try:
        writer.write(df, path, **option_kwargs)
    except AuthError as err:
        err.print_warning()


def show_meta(path):
    dataset_doc = DatasetDocClient(AuthClient.get_access_token, os.environ['DOC_TEMPLATE_URL'])
    try:
        return dataset_doc.get_meta(path)
    except AuthError as err:
        err.print_warning()
