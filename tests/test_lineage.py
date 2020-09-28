import unittest
import json
import os

from pyspark.sql import SparkSession
from dapla.magics.lineage import map_lineage


def resolve_filename(filename):
    return os.path.join(os.path.dirname(__file__), filename)


class DaplaLineageMagicsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._spark = SparkSession.builder.config("spark.ui.enabled", False).getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls._spark.sparkContext.stop()

    def test_lineage_output(self):
        with open(resolve_filename('lineage_template.json'), 'r') as f:
            lineage_template = json.load(f)
        with open(resolve_filename('lineage_output.json'), 'r') as f:
            expected_lineage = json.load(f)

        output = map_lineage(lineage_template)
        self.assertEqual(expected_lineage, output)
