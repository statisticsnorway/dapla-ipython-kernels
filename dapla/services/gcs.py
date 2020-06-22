import gcsfs
from google.oauth2.credentials import Credentials


class GCSFileSystem(gcsfs.GCSFileSystem):

    def __init__(self, access_token, access):
        token = Credentials(
            access_token,
            token_uri="https://oauth2.googleapis.com/token",
        )
        super().__init__(token=token, access=access)
        # Temporary bug fix for https://issues.apache.org/jira/browse/ARROW-7867
        # Spark writes an empty file to GCS (to mimic a folder structure) before writing partitioned data
        # Resolve this by ignoring the "empty" file when reading partitioned parquet files
        from pyarrow.parquet import EXCLUDED_PARQUET_PATHS
        from pyarrow.parquet import ParquetManifest
        EXCLUDED_PARQUET_PATHS.add('')
        ParquetManifest._should_silently_exclude = _should_silently_exclude
        # Another bug https://issues.apache.org/jira/browse/ARROW-1644


    def isdir(self, path):
        info = super(gcsfs.GCSFileSystem, self).info(path)
        return info["type"] == "directory"

def _should_silently_exclude(self, file_name):
    from pyarrow.parquet import EXCLUDED_PARQUET_PATHS
    return (file_name.endswith('.crc') or  # Checksums
            file_name.endswith('_$folder$') or  # HDFS directories in S3
            file_name.startswith('.') or  # Hidden files starting with .
            file_name.startswith('_') or  # Hidden files starting with _
            '.tmp' in file_name or  # Temp files
            file_name in EXCLUDED_PARQUET_PATHS)

class GCSConnector:
    """
    Connect to Dapla's Google Cloud Storage.
    """
    def __init__(
            self,
            user_token,
            data_access_url=None,
            catalog_url=None,
            metadata_publisher_url=None,
    ):
        """
        Args:
            user_token (Optional(str)): The OAuth 2.0 access token. When user_token=None,
                no authentication is performed, and you can only access public data
            data_access_url (str): Endpoint to dapla's data access service
            catalog_url (str): Endpoint to dapla's catalog service
            metadata_publisher_url (str): Endpoint to dapla's metadata publisher service
        """
        self._user_token = user_token
        self._data_access_url = data_access_url
        self._catalog_url = catalog_url
        self._metadata_publisher_url = metadata_publisher_url

    def read(self, path):
        if self._user_token is None:
            token = 'anon'
        else:
            token = Credentials(
                None,
                token_uri="https://oauth2.googleapis.com/token",
            )
        fs = gcsfs.GCSFileSystem(token=token)
        return fs.read_parquet(path)


