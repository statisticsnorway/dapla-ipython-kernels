from .clients import DataAccessClient, DataAccessError, MetadataPublisherClient
from .gcs import GCSFileSystem
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
    ):
        """
        Args:
            user_token (Optional(str)): The OAuth 2.0 access token. When user_token=None,
                no authentication is performed, and you can only access public data
            data_access_url (str): Endpoint to dapla's data access service
        """
        self._data_access_client = DataAccessClient(user_token_provider, data_access_url)
        self._metadata_publisher_client = MetadataPublisherClient(
            user_token_provider, metadata_publisher_url,
            metadata_publisher_project_id, metadata_publisher_topic_name)

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
        if not location_response['accessAllowed']:
            raise DataAccessError("Din bruker har ikke tilgang")
        else:
            gcs_path = "{}{}/{}".format(location_response['parentUri'], path, version)
            fs = GCSFileSystem(location_response['accessToken'], "read_write")

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
