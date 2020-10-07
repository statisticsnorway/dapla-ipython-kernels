import responses
import unittest
from unittest.mock import MagicMock
from io import StringIO
from os.path import dirname

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from IPython.core.error import UsageError

from dapla.magics.documentation import DaplaDocumentationMagics
from dapla.services.clients import DatasetDocClient


class DaplaDocumentationMagicsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._spark = SparkSession.builder.config("spark.ui.enabled", False).getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls._spark.sparkContext.stop()

    def setUp(self):
        doc_template_client = DatasetDocClient(lambda: 'mock-user-token', 'http://mock.no/')
        self._magic = DaplaDocumentationMagics(None, doc_template_client.get_doc_template)
        self._magic.shell = MagicMock()
        self._magic.display = MagicMock()

        schema = StructType([
            StructField('variable1', StringType(), True)
        ])
        spark_dataframe = self._spark.createDataFrame([], schema)
        self._magic.shell.user_ns = {"ds": spark_dataframe}

    def test_missing_variable_name(self):
        with self.assertRaisesRegex(UsageError, 'Missing variable name.'):
            # Run the magic
            self._magic.document('-f output/docs/mockfileinput.json')

    # skip because write to file fails on CI server
    @unittest.skip
    @responses.activate
    def test_generate_doc_template(self):
        responses.add(responses.POST, 'http://mock.no/doc/template',
                      json=doc_template, status=200)
        output_file = "{}/output/docs/mockfile.json".format(dirname(__file__))
        # Mock that the user inputs a file name
        self._magic.shell.ev = MagicMock(return_value=output_file)
        # Run the magic
        self._magic.document('ds')
        # Check that the user was asked for file input
        self._magic.shell.ev.assert_called_with('input("Enter filename where the documentation should be stored")')
        # Check that cell content was updated
        self._magic.shell.set_next_input.assert_called_with('%document -f {} ds'.format(output_file), replace=True)

    @responses.activate
    def test_generate_doc_template_no_file(self):
        responses.add(responses.POST, 'http://mock.no/doc/template',
                      json=doc_template, status=200)
        # Run the magic
        self._magic.document('--nofile ds')
        # Capture the display output
        captor = StringIO()
        print(*self._magic.display.call_args[0], file=captor, flush=True)
        print(captor.getvalue())
        self.assertEqual(expected_widgets, captor.getvalue())


doc_template = {
    "name": "ds name",
    "description": "ds description",
    'unitType': {'concept-type': 'UnitType',
                 'selected-id': 'UnitType_DUMMY',
                 'candidates': [
                     {'id': 'UnitType_DUMMY', 'name': 'UnitType_DUMMY'}]},
    "instanceVariables": [
        {
            "name": "iv1",
            "description": "iv1descr",
            "checkboxValue": False,
            "enumValue": {
                "selected-enum": "VAL2",
                "enums": [
                    "VAL1",
                    "VAL2",
                    "VAL3"
                ]
            },
            "selectionValue": {
                "selected-id": "id3",
                "candidates": [
                    {"id": "id1", "name": "name1"},
                    {"id": "id2", "name": "name2"},
                    {"id": "id3", "name": "name3"}
                ]
            }
        }
    ]
}

expected_widgets = "VBox(children=(HTML(value='<b style=\"font-size:14px\">Dataset metadata</b>'), \
Box(children=(Box(children=(Label(value='Name'), Text(value='ds name')), \
layout=Layout(display='flex', flex_flow='row', justify_content='space-between')), \
Box(children=(Label(value='Description'), Textarea(value='ds description')), \
layout=Layout(display='flex', flex_flow='row', justify_content='space-between')), \
Box(children=(Label(value='UnitType'), Dropdown(options=(('UnitType_DUMMY', 'UnitType_DUMMY'),), value='UnitType_DUMMY')), \
layout=Layout(display='flex', flex_flow='row', justify_content='space-between'))), \
layout=Layout(align_items='stretch', display='flex', flex_flow='column', width='70%')), \
HTML(value='<b style=\"font-size:14px\">Instance variables</b>'), \
Accordion(children=(Box(children=(Box(children=(Label(value='Description'), \
Textarea(value='iv1descr')), layout=Layout(display='flex', flex_flow='row', justify_content='space-between')), \
Box(children=(Label(value='CheckboxValue'), Checkbox(value=False, indent=False)), \
layout=Layout(display='flex', flex_flow='row', justify_content='space-between')), \
Box(children=(Label(value='EnumValue'), Dropdown(index=1, options=('VAL1', 'VAL2', 'VAL3'), value='VAL2')), \
layout=Layout(display='flex', flex_flow='row', justify_content='space-between')), \
Box(children=(Label(value='SelectionValue'), \
Dropdown(index=2, options=(('name1', 'id1'), ('name2', 'id2'), ('name3', 'id3')), value='id3')), \
layout=Layout(display='flex', flex_flow='row', justify_content='space-between'))), \
layout=Layout(align_items='stretch', display='flex', flex_flow='column', width='70%')),), selected_index=None, \
_titles={'0': 'Iv1'})))\n"

