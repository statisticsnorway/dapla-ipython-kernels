from .clients import DataAccessClient, DataAccessError, CatalogClient
from .gcs import GCSFileSystem
import pyarrow.parquet
# import utils to register the pyarrow extension types
import pandas.core.arrays._arrow_utils  # noqa


class DataSourceReader:
    """
    Connect to Dapla's Google Cloud Storage.
    """
    def __init__(
            self,
            user_token_provider,
            data_access_url=None,
            catalog_url=None,
    ):
        """
        Args:
            user_token (Optional(str)): The OAuth 2.0 access token. When user_token=None,
                no authentication is performed, and you can only access public data
            data_access_url (str): Endpoint to dapla's data access service
        """
        self._data_access_client = DataAccessClient(user_token_provider, data_access_url)
        self._catalog_client = CatalogClient(user_token_provider, catalog_url)

    def list(self, path):
        import pandas as pd
        df = pd.DataFrame(self._catalog_client.list(path)['entries'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df

    def details(self, path):
        fs, gcs_path = self._get_fs(path)
        import pandas as pd
        return pd.DataFrame(list(map(DataSourceReader._file_item(gcs_path), fs.listdir(gcs_path, True))))

    @staticmethod
    def _file_item(gcs_path):
        return lambda o: {'Size': o['size'], 'Name': DataSourceReader._trimmed_name(gcs_path, o)}

    @staticmethod
    def _trimmed_name(gcs_path, o):
        return o['name'].lstrip(o['bucket']).lstrip(gcs_path)

    def read(self, path, columns=None, to_pandas=True, **kwargs):
        fs, gcs_path = self._get_fs(path)
        parquet_ds = pyarrow.parquet.ParquetDataset(
            gcs_path, filesystem=fs, **kwargs
        )
        if to_pandas:
            kwargs["columns"] = columns
            return parquet_ds.read_pandas(**kwargs).to_pandas(split_blocks=False, self_destruct=True)
        else:
            return fs.read_parquet(gcs_path, columns=columns)

    def _get_fs(self, path):
        # Get full path to latest version in GCS bucket
        location_response = self._data_access_client.read_location(path)
        if not location_response['accessAllowed']:
            raise DataAccessError("Din bruker har ikke tilgang")

        parent_uri = location_response['parentUri']
        if parent_uri.startswith('gs:'):
            fs = GCSFileSystem(location_response['accessToken'], "read_only")
            gcs_path = "{}{}/{}".format(parent_uri, path, location_response['version'])
            return fs, gcs_path
        elif parent_uri.startswith('file:'):
            gcs_path = "{}{}/{}".format(parent_uri.lstrip('file:').replace('//', ''), path, location_response['version'])
            return None, gcs_path
        else:
            raise DataAccessError("Unknown file scheme: " + parent_uri)
