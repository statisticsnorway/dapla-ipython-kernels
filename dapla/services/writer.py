from .clients import DataAccessClient, DataAccessError, MetadataPublisherClient, CatalogClient
from .gcs import GCSFileSystem
from pathlib import Path
import pyarrow.parquet
import base64
# import utils to register the pyarrow extension types
import pandas.core.arrays._arrow_utils  # noqa

class DataSourceWriter:
    """
    Connect to Dapla's Google Cloud Storage.
    """
    def __init__(
            self,
            user_token_provider,
            data_access_url=None,
            metadata_publisher_url=None,
            metadata_publisher_project_id=None,
            metadata_publisher_topic_name=None,
            catalog_url=None,
    ):
        """
        Args:
            user_token (Optional(str)): The OAuth 2.0 access token. When user_token=None,
                no authentication is performed, and you can only access public data
            data_access_url (str): Endpoint to dapla's data access service
            metadata_publisher_url (str): Endpoint to dapla's data metadata publisher service
            metadata_publisher_project_id (str): Project ID used by metadata publisher service
            metadata_publisher_topic_name (str): Topic name used by metadata publisher service
            catalog_url (str): Endpoint to dapla's data catalog service
        """
        self._data_access_client = DataAccessClient(user_token_provider, data_access_url)
        self._metadata_publisher_client = MetadataPublisherClient(
            user_token_provider, metadata_publisher_url,
            metadata_publisher_project_id, metadata_publisher_topic_name)
        self._catalog_client = CatalogClient(user_token_provider, catalog_url)

    @staticmethod
    def _current_milli_time():
        import time
        return int(round(time.time() * 1000))

    @staticmethod
    def guid():
        from uuid import uuid4
        return uuid4().hex

    def write(self, df, path, **kwargs):
        version = kwargs.pop("version", self._current_milli_time())
        valuation = kwargs.pop("valuation")
        state = kwargs.pop("state")

        location_response = self._data_access_client.write_location(path, version, valuation, state)
        fs, gcs_path = self._get_fs(location_response, path, version)
        pandas.io.parquet.BaseImpl.validate_dataframe(df)

        # Write metadata file
        with fs.open(gcs_path + "/.dataset-meta.json", mode="wb") as buffer:
            valid_metadata = base64.b64decode(location_response["validMetadataJson"])
            buffer.write(valid_metadata)

        # Transfom and write pandas dataframe
        from_pandas_kwargs = {"schema": kwargs.pop("schema", None)}
        #if index is not None:
        #    from_pandas_kwargs["preserve_index"] = index
        table = pyarrow.Table.from_pandas(df, preserve_index=True, **from_pandas_kwargs)
        with fs.open("{}/{}.parquet".format(gcs_path, self.guid()), mode="wb") as buffer:
            pyarrow.parquet.write_table(
                table,
                buffer,
                compression="snappy",
                coerce_timestamps="ms",
                **kwargs,
            )
        # Write metadata signature file
        with fs.open(gcs_path + "/.dataset-meta.json.sign", mode="wb") as buffer:
            valid_metadata = base64.b64decode(location_response["metadataSignature"])
            buffer.write(valid_metadata)

        # Publish metadata signature file created event, this will be used for validation and signals a "commit" of metadata
        self._metadata_publisher_client.data_changed(gcs_path + "/.dataset-meta.json.sign")

        # Update catalog
        self._catalog_client.write_dataset(path, version, valuation, state,
                                           location_response['parentUri'],
                                           location_response["validMetadataJson"],
                                           location_response["metadataSignature"])

    def _get_fs(self, location_response, path, version):
        if not location_response['accessAllowed']:
            raise DataAccessError("Din bruker har ikke tilgang")
        parent_uri = location_response['parentUri']
        if parent_uri.startswith('gs:'):
            gcs_path = "{}{}/{}".format(location_response['parentUri'], path, version)
            fs = GCSFileSystem(location_response['accessToken'], "read_write")
            return fs, gcs_path
        elif parent_uri.startswith('file:'):
            gcs_path = "{}{}/{}".format(parent_uri.lstrip('file:').replace('//', ''), path, version)
            # Create anonymous class to wrap Python's build-in open() function to open a file
            fs = type("", (), dict(open=lambda f, mode: open(f, mode)))
            # Ensure that directory exists
            Path(gcs_path).mkdir(parents=True, exist_ok=True)
            return fs, gcs_path
        else:
            raise DataAccessError("Unknown file scheme: " + parent_uri)
