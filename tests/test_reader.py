from dapla.services.reader import DataAccessClient, DataSourceReader
import unittest
import os
import responses

# For use in integration tests
access_token = 'TODO'


class ReaderTest(unittest.TestCase):
    @unittest.skip
    def test_read_from_path(self):
        reader = DataSourceReader(lambda: access_token,
                                  'https://data-access.staging-bip-app.ssb.no/',
                                  'https://dapla-catalog.staging-bip-app.ssb.no')
        table = reader.read('/felles/bjornandre/python/test1')
        print(table.head(5))

    @unittest.skip
    def test_list_path(self):
        reader = DataSourceReader(lambda: access_token,
                                  'https://data-access.staging-bip-app.ssb.no/',
                                  'https://dapla-catalog.staging-bip-app.ssb.no')
        paths = reader.list('/felles/bjornandre/python/')
        print(paths)

    @unittest.skip
    def test_details(self):
        reader = DataSourceReader(lambda: access_token,
                                  'https://data-access.staging-bip-app.ssb.no/',
                                  'https://dapla-catalog.staging-bip-app.ssb.no')
        paths = reader.details('/felles/bjornandre/python/test1')
        print(paths)

    @responses.activate
    def test_read_file(self):
        json_response = {
            'accessAllowed': True,
            'parentUri': 'file://{}'.format(os.path.dirname(__file__)),
            'version': '1591300975435',
            'accessToken': 'mock-access-token',
            'expirationTime': '1592492757749'
        }
        responses.add(responses.POST, 'http://mock.no/rpc/DataAccessService/readLocation',
                      json=json_response, status=200)
        reader = DataSourceReader(lambda: 'mock-user-token',
                                  'http://mock.no/',
                                  'http://mock.no/')
        table = reader.read('/testdata')
        print(table.head(5))

