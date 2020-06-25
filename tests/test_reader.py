from dapla.services.reader import DataAccessClient, DataSourceReader
import unittest

access_token = 'TODO'

@unittest.skip
class ReaderTest(unittest.TestCase):
    def test_read_from_path(self):
        reader = DataSourceReader(lambda: access_token,
                                  'https://data-access.staging-bip-app.ssb.no/',
                                  'https://dapla-catalog.staging-bip-app.ssb.no')
        table = reader.read('/felles/bjornandre/python/test1')
        print(table.head(5))

    def test_list_path(self):
        reader = DataSourceReader(lambda: access_token,
                                  'https://data-access.staging-bip-app.ssb.no/',
                                  'https://dapla-catalog.staging-bip-app.ssb.no')
        paths = reader.list('/felles/bjornandre/python/')
        print(paths)

    def test_details(self):
        reader = DataSourceReader(lambda: access_token,
                                  'https://data-access.staging-bip-app.ssb.no/',
                                  'https://dapla-catalog.staging-bip-app.ssb.no')
        paths = reader.details('/felles/bjornandre/python/test1')
        print(paths)
