import os

from dapla.jupyterextensions.authextension import AuthClient
from dapla.services.reader import DataSourceReader
from dapla.services.writer import DataSourceWriter


def read_pandas(path):
    reader = DataSourceReader(AuthClient.get_access_token, os.environ['DATA_ACCESS_URL'],  os.environ['CATALOG_URL'])
    return reader.read(path)


def show(path):
    reader = DataSourceReader(AuthClient.get_access_token, os.environ['DATA_ACCESS_URL'],  os.environ['CATALOG_URL'])
    return reader.list(path)


def details(path):
    reader = DataSourceReader(AuthClient.get_access_token, os.environ['DATA_ACCESS_URL'],  os.environ['CATALOG_URL'])
    return reader.details(path)


def write_pandas(df, path, **option_kwargs):
    writer = DataSourceWriter(
        AuthClient.get_access_token,
        os.environ['DATA_ACCESS_URL'],
        os.environ['METADATA_PUBLISHER_URL'],
        os.environ['METADATA_PUBLISHER_PROJECT_ID'],
        os.environ['METADATA_PUBLISHER_TOPIC_NAME'],
        os.environ['CATALOG_URL']
    )
    writer.write(df, path, **option_kwargs)
