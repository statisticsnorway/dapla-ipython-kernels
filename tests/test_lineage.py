import responses
import unittest
from unittest.mock import MagicMock
from io import StringIO
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from dapla.magics.lineage import map_lineage, DaplaLineageMagics
from dapla.services.clients import DatasetDocClient
from tests import resolve_filename


class DaplaLineageMagicsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._spark = SparkSession.builder.config("spark.ui.enabled", False).getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls._spark.sparkContext.stop()

    def setUp(self):
        doc_template_client = DatasetDocClient(lambda: 'mock-user-token', 'http://mock.no/')
        self._magic = DaplaLineageMagics(None, doc_template_client.get_lineage_template)
        self._magic.shell = MagicMock()
        self._magic.display = MagicMock()

    @responses.activate
    def test_generate_lineage_template(self):
        with open(resolve_filename('lineage_template.json'), 'r') as f:
            lineage_template = json.load(f)
        responses.add(responses.POST, 'http://mock.no/lineage/template',
                      json=lineage_template, status=200)
        # Create 3 input datasets
        person_type = StructType([
            StructField('personidentifikator', StringType()),
            StructField('kontonummer', StringType())])
        person = self._spark.createDataFrame([], person_type)
        unrelated_type = StructType([
            StructField('weird', StringType()),
            StructField('stuff', StringType())])
        unrelated = self._spark.createDataFrame([], unrelated_type)
        konto_type = StructType([
            StructField('kontonummer', StringType()),
            StructField('innskudd', IntegerType())])
        konto = self._spark.createDataFrame([], konto_type)
        # Register inputs
        self._magic.input('', '/skatt/person\n/skatt/konto\n/skatt/unrelated')
        # Load inputs
        self._magic.on_input_load('/skatt/person {} {}'.format(1111, person.schema.json()))
        self._magic.on_input_load('/skatt/konto {} {}'.format(1111, konto.schema.json()))
        self._magic.on_input_load('/skatt/unrelated {} {}'.format(1111, unrelated.schema.json()))

        # Output dataset
        innskudd = person.join(konto, 'kontonummer', how='inner')
        self._magic.shell.user_ns = {"innskudd": innskudd}
        # Run the lineage magic
        self._magic.lineage('--nofile innskudd')
        # Capture the display output
        captor = StringIO()
        print(*self._magic.display.call_args[0], file=captor, flush=True)
        print(captor.getvalue())

    def test_lineage_output(self):
        with open(resolve_filename('lineage_template.json'), 'r') as f:
            lineage_template = json.load(f)
        with open(resolve_filename('lineage_output.json'), 'r') as f:
            expected_lineage = json.load(f)

        output = map_lineage(lineage_template)
        self.assertEqual(expected_lineage, output)

    def test_lineage_output_no_source(self):
        with open(resolve_filename('lineage_template_no_source.json'), 'r') as f:
            lineage_template = json.load(f)
        with open(resolve_filename('lineage_output_no_source.json'), 'r') as f:
            expected_lineage = json.load(f)

        output = map_lineage(lineage_template)
        self.assertEqual(expected_lineage, output)
