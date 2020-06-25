from dapla.services.reader import DataSourceReader
from dapla.services.writer import DataSourceWriter
import unittest

access_token = 'TODO'

@unittest.skip
class WriterTest(unittest.TestCase):
    def test_read_write(self):
        reader = DataSourceReader(lambda: access_token,
                                  'https://data-access.staging-bip-app.ssb.no/',
                                  'https://dapla-catalog.staging-bip-app.ssb.no')
        table = reader.read('/felles/bjornandre/bilmerker_og_innskudd')
        print(table.head(5))

        writer = DataSourceWriter(lambda: access_token,
                                  'https://data-access.staging-bip-app.ssb.no/',
                                  'https://metadata-distributor.staging-bip-app.ssb.no',
                                  'staging-bip',
                                  'metadata-distributor-dataset-updates'
                                  )
        writer.write(table, '/felles/bjornandre/python/test1', valuation="INTERNAL", state="INPUT")
