from dapla.services.reader import DataAccessClient, DataSourceReader
import unittest
import responses

access_token = 'TODO'

@unittest.skip
class ReaderTest(unittest.TestCase):
    def test_data_access_client(self):
        reader = DataSourceReader(access_token, 'https://data-access.staging-bip-app.ssb.no/')
        table = reader.read('/felles/bjornandre/test/fødsel')
        print(table.head(5))


