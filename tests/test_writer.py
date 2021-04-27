from dapla.services.reader import DataSourceReader
from dapla.services.writer import DataSourceWriter
import os
import pandas as pd
import json
import base64
import responses
import unittest


class WriterTest(unittest.TestCase):
    @unittest.skip
    def test_read_write_gcs(self):
        access_token = 'TODO'
        reader = DataSourceReader(lambda: access_token,
                                  'https://data-access.staging-bip-app.ssb.no/',
                                  'https://dapla-catalog.staging-bip-app.ssb.no')
        table = reader.read('/felles/bjornandre/bilmerker_og_innskudd')
        print(table.head(5))

        writer = DataSourceWriter(lambda: access_token,
                                  'https://data-access.staging-bip-app.ssb.no/',
                                  'https://metadata-distributor.staging-bip-app.ssb.no',
                                  'staging-bip',
                                  'metadata-distributor-dataset-updates',
                                  'https://dapla-catalog.staging-bip-app.ssb.no',
                                  )
        writer.write(table, '/felles/bjornandre/python/test1', valuation="INTERNAL", state="INPUT")

    @responses.activate
    def test_read_write_file(self):
        table = pd.read_parquet('{}/testdata/1591300975435'.format(os.path.dirname(__file__)))
        print(table.head(5))
        json_response = {
            'accessAllowed': True,
            'parentUri': 'file://{}'.format(os.path.dirname(__file__)),
            'accessToken': 'mock-access-token',
            'validMetadataJson': base64.b64encode(json.dumps({'key': 'value'}).encode('ascii')).decode('ascii'),
            'allValidMetadataJson': base64.b64encode(json.dumps({'key': 'value'}).encode('ascii')).decode('ascii'),
            'metadataSignature': base64.b64encode(json.dumps({'key': 'value'}).encode('ascii')).decode('ascii'),
            'allMetadataSignature': base64.b64encode(json.dumps({'key': 'value'}).encode('ascii')).decode('ascii')
        }
        responses.add(responses.POST, 'http://mock.no/rpc/DataAccessService/writeLocation',
                      json=json_response, status=200)
        responses.add(responses.POST, 'http://mock.no/rpc/MetadataDistributorService/dataChanged',
                      status=200)
        responses.add(responses.POST, 'http://mock.no/catalog/write',
                      status=200)

        writer = DataSourceWriter(lambda: 'mock-user-token',
                                  'http://mock.no/',
                                  'http://mock.no/',
                                  'project-bip',
                                  'topic-bip',
                                  'http://mock.no/'
                                  )
        writer.write(table, '/output/dataset', valuation="INTERNAL", state="INPUT")
