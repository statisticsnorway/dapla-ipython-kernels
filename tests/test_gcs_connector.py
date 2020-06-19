from unittest.mock import MagicMock
from services.gcs import GCSConnector

def test_anonymous_read():
    gcs_connector = GCSConnector(user_token=None)
    table = gcs_connector.read('gs://anaconda-public-data/nyc-taxi/nyc.parquet/part.0.parquet')
    print(table.to_pandas().head(5))
    pass


