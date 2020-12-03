from dapla.services.clients import DataAccessClient, DataAccessError, DatasetDocClient
import responses
import unittest
import time


class ClientsTest(unittest.TestCase):
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

    @responses.activate
    def test_error_handling(self):
        responses.add(responses.GET, 'http://mock.no/doc/candidates/UnitType',
                      json={'test': 'test-a'}, status=200)
        client = DatasetDocClient(lambda: 'mock-user-token', 'http://mock.no/')
        for i in range(10):
            client.get_doc_template_candidates('unitType')
        assert len(responses.calls) == 1

    @unittest.skip  # for testing against local running service
    def test_dataset_doc_client(self):
        client = DatasetDocClient(lambda: 'mock-user-token', 'http://localhost:10190/')

        for i in range(20):
            candidates = client.get_doc_template_candidates('unitType')
            print(candidates)
            time.sleep(0.2)
