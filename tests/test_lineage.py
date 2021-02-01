import responses
import unittest
from unittest.mock import MagicMock
from io import StringIO
import json
import re
import os

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
        checked = self.setup_and_call_lineage('--nofile innskudd')
        expected = ['<b>kontonummer</b>',
                    "Label(value='/skatt/person')",
                    "Checkbox(value=False, description='kontonummer (*)'",
                    "Checkbox(value=False, description='personidentifikator'",
                    "Label(value='/skatt/konto')",
                    "Checkbox(value=False, description='kontonummer (*)'",
                    "Checkbox(value=False, description='innskudd'",
                    "Label(value='/skatt/unrelated')",
                    "Checkbox(value=False, description='weird'",
                    "Checkbox(value=False, description='stuff'",
                    '<b>personidentifikator</b>',
                    "Label(value='/skatt/person')",
                    "Checkbox(value=False, description='personidentifikator (*)'",
                    "Checkbox(value=False, description='kontonummer'",
                    "Label(value='/skatt/konto')",
                    "Checkbox(value=False, description='kontonummer'",
                    "Checkbox(value=False, description='innskudd'",
                    "Label(value='/skatt/unrelated')",
                    "Checkbox(value=False, description='weird'",
                    "Checkbox(value=False, description='stuff'",
                    '<b>innskudd</b>',
                    "Label(value='/skatt/person')",
                    "Checkbox(value=False, description='personidentifikator'",
                    "Checkbox(value=False, description='kontonummer'",
                    "Label(value='/skatt/konto')",
                    "Checkbox(value=False, description='innskudd (*)'",
                    "Checkbox(value=False, description='kontonummer'",
                    "Label(value='/skatt/unrelated')",
                    "Checkbox(value=False, description='weird'",
                    "Checkbox(value=False, description='stuff'"]
        self.assertEqual(expected, checked)

    @responses.activate
    def test_read_lineage_template_and_populate_controls(self):
        checked = self.setup_and_call_lineage('-f {} innskudd'.format(
            resolve_filename('selected_innskudd_lineage.json')))
        expected = ['<b>kontonummer</b>',
                    "Label(value='/skatt/person')",
                    "Checkbox(value=True, description='kontonummer (*)'",
                    "Checkbox(value=False, description='personidentifikator'",
                    "Label(value='/skatt/konto')",
                    "Checkbox(value=True, description='kontonummer (*)'",
                    "Checkbox(value=False, description='innskudd'",
                    "Label(value='/skatt/unrelated')",
                    "Checkbox(value=False, description='weird'",
                    "Checkbox(value=False, description='stuff'",
                    '<b>personidentifikator</b>',
                    "Label(value='/skatt/person')",
                    "Checkbox(value=True, description='personidentifikator (*)'",
                    "Checkbox(value=False, description='kontonummer'",
                    "Label(value='/skatt/konto')",
                    "Checkbox(value=False, description='kontonummer'",
                    "Checkbox(value=False, description='innskudd'",
                    "Label(value='/skatt/unrelated')",
                    "Checkbox(value=False, description='weird'",
                    "Checkbox(value=False, description='stuff'",
                    '<b>innskudd</b>',
                    "Label(value='/skatt/person')",
                    "Checkbox(value=False, description='personidentifikator'",
                    "Checkbox(value=False, description='kontonummer'",
                    "Label(value='/skatt/konto')",
                    "Checkbox(value=True, description='innskudd (*)'",
                    "Checkbox(value=False, description='kontonummer'",
                    "Label(value='/skatt/unrelated')",
                    "Checkbox(value=False, description='weird'",
                    "Checkbox(value=False, description='stuff'"]
        self.assertEqual(expected, checked)

    @responses.activate
    def test_read_lineage_template_and_populate_controls_only_skatt_konto_should_be_selected(self):
        checked = self.setup_and_call_lineage('-f {} innskudd '.format(
            resolve_filename('selected_only_skatt_konto_innskudd_lineage.json')))
        expected = ['<b>kontonummer</b>',
                    "Label(value='/skatt/person')",
                    "Checkbox(value=False, description='kontonummer (*)'",
                    "Checkbox(value=False, description='personidentifikator'",
                    "Label(value='/skatt/konto')",
                    "Checkbox(value=True, description='kontonummer (*)'",
                    "Checkbox(value=False, description='innskudd'",
                    "Label(value='/skatt/unrelated')",
                    "Checkbox(value=False, description='weird'",
                    "Checkbox(value=False, description='stuff'",
                    '<b>personidentifikator</b>',
                    "Label(value='/skatt/person')",
                    "Checkbox(value=True, description='personidentifikator (*)'",
                    "Checkbox(value=False, description='kontonummer'",
                    "Label(value='/skatt/konto')",
                    "Checkbox(value=False, description='kontonummer'",
                    "Checkbox(value=False, description='innskudd'",
                    "Label(value='/skatt/unrelated')",
                    "Checkbox(value=False, description='weird'",
                    "Checkbox(value=False, description='stuff'",
                    '<b>innskudd</b>',
                    "Label(value='/skatt/person')",
                    "Checkbox(value=False, description='personidentifikator'",
                    "Checkbox(value=False, description='kontonummer'",
                    "Label(value='/skatt/konto')",
                    "Checkbox(value=True, description='innskudd (*)'",
                    "Checkbox(value=False, description='kontonummer'",
                    "Label(value='/skatt/unrelated')",
                    "Checkbox(value=False, description='weird'",
                    "Checkbox(value=False, description='stuff'"]
        self.assertEqual(expected, checked)

    @responses.activate
    def test_create_default_file_when_missing_filename_args(self):
        self.setup_and_call_lineage('innskudd')
        file_name = 'lineage_innskudd.json'
        if os.path.isfile(file_name):
            print(file_name)
            os.remove(file_name)
        else:
            self.fail("expected file not created {}".format(file_name))


    def setup_and_call_lineage(self, linage_args):
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
        self._magic.lineage(linage_args)
        # Capture the display output
        captor = StringIO()
        print(*self._magic.display.call_args[0], file=captor, flush=True)
        controls = captor.getvalue()
        regexp = "<b>\w+</b>|Label\(value='/\w+/\w+'\)|Checkbox\(value=\w{4}\w?, description='\w+(?:.{4})?'"
        return re.findall(regexp, controls)

    def test_parse_options(self):
        opts, args = self._magic.parse_options('', '', 'append')
        self.assertFalse('append' in opts)
        opts, args = self._magic.parse_options('--append', '', 'append')
        self.assertTrue('append' in opts)

    def test_disable_input_warning(self):
        self.assertTrue(self._magic._show_warning_input)
        self._magic.input_warning('off')
        self.assertFalse(self._magic._show_warning_input)
        self._magic.input_warning('on')
        self.assertTrue(self._magic._show_warning_input)

    def test_disable_output_warning(self):
        self.assertTrue(self._magic._show_warning_output)
        self._magic.output_warning('False')
        self.assertFalse(self._magic._show_warning_output)
        self._magic.output_warning('True')
        self.assertTrue(self._magic._show_warning_output)

    def test_lineage_output(self):
        with open(resolve_filename('selected_innskudd_lineage.json'), 'r') as f:
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
