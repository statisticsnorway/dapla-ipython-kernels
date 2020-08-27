from dapla.services.clients import DataAccessClient, DataAccessError
import responses
import unittest


class ClientsTest(unittest.TestCase):
    # skip because write to file fails on CI server
    @unittest.skip
    @responses.activate
    def test_data_access_client(self):
        json_response = {
            'accessAllowed': True,
            'parentUri': 'gs://ssb-data-test',
            'version': '1591300975435',
            'accessToken': 'mock-access-token',
            'expirationTime': '1592492757749'
        }
        responses.add(responses.POST, 'http://mock.no/rpc/DataAccessService/readLocation',
                      json=json_response, status=200)
        client = DataAccessClient(lambda: 'mock-user-token', 'http://mock.no/')
        location = client.read_location('/user/test')
        assert location['parentUri'] == 'gs://ssb-data-test'

    @responses.activate
    def test_error_handling(self):
        responses.add(responses.POST, 'http://mock.no/rpc/DataAccessService/readLocation',
                      json={'error': 'not found'}, status=404)
        with self.assertRaisesRegex(DataAccessError, 'Fant ikke datasett'):
            client = DataAccessClient(lambda: 'mock-user-token', 'http://mock.no/')
            client.read_location('/user/test')



