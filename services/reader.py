from .clients import DataAccessClient, DataAccessError
from .gcs import GCSFileSystem


class DataSourceReader:
    """
    Connect to Dapla's Google Cloud Storage.
    """
    def __init__(
            self,
            user_token,
            data_access_url=None,
    ):
        """
        Args:
            user_token (Optional(str)): The OAuth 2.0 access token. When user_token=None,
                no authentication is performed, and you can only access public data
            data_access_url (str): Endpoint to dapla's data access service
        """
        self._client = DataAccessClient(user_token, data_access_url)

    def read(self, path):
        # Get full path to latest version in GCS bucket
        location_response = self._client.read_location(path)
        if not location_response['accessAllowed']:
            raise DataAccessError("Din bruker har ikke tilgang")
        else:
            gcs_path = "{}{}/{}".format(location_response['parentUri'], path, location_response['version'])
            fs = GCSFileSystem(location_response['accessToken'], "read_only")
            return fs.read_parquet(gcs_path)


