import unittest
from unittest.mock import MagicMock
from io import StringIO

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from IPython.core.error import UsageError

from dapla.magics.documentation import DaplaDocumentationMagics

doc_template = {
    "name": "ds name",
    "description": "ds description",
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

expected_widgets = "HTML(value='<b>Dataset metadata</b>') \
Box(children=(Box(children=(Label(value='Name'), Text(value='ds name')), \
layout=Layout(display='flex', flex_flow='row', justify_content='space-between')), \
Box(children=(Label(value='Description'), Textarea(value='ds description')), \
layout=Layout(display='flex', flex_flow='row', justify_content='space-between'))), \
layout=Layout(align_items='stretch', display='flex', flex_flow='column', width='50%')) \
HTML(value='<b>Instance variables</b>') \
Accordion(children=(Box(children=(Box(children=(Label(value='Description'), \
Textarea(value='iv1descr')), layout=Layout(display='flex', flex_flow='row', justify_content='space-between')), \
Box(children=(Label(value='Checkboxvalue'), Checkbox(value=False, indent=False)), \
layout=Layout(display='flex', flex_flow='row', justify_content='space-between')), \
Box(children=(Label(value='Enumvalue'), Dropdown(index=1, options=('VAL1', 'VAL2', 'VAL3'), value='VAL2')), \
layout=Layout(display='flex', flex_flow='row', justify_content='space-between')), \
Box(children=(Label(value='Selectionvalue'), \
Dropdown(index=2, options=(('name1', 'id1'), ('name2', 'id2'), ('name3', 'id3')), value='id3')), \
layout=Layout(display='flex', flex_flow='row', justify_content='space-between'))), \
layout=Layout(align_items='stretch', display='flex', flex_flow='column', width='50%')),), _titles={'0': 'Iv1'}) \
Button(description='Save to file', icon='file-code', style=ButtonStyle()) \
Output()\n"


class DaplaDocumentationMagicsTest(unittest.TestCase):

    def setUp(self):
        self._spark = SparkSession.builder.config("spark.ui.enabled", False).getOrCreate()
        self._magic = DaplaDocumentationMagics(None, lambda ds, simple: json.dumps(doc_template))
        self._magic.shell = MagicMock()
        self._magic.display = MagicMock()

        schema = StructType([
            StructField('variable1', StringType(), True)
        ])
        df = self._spark.createDataFrame([], schema)
        self._magic.shell.user_ns = {"ds": df}

    def tearDown(self) -> None:
        self._spark.sparkContext.stop()

    def test_missing_variable_name(self):
        with self.assertRaisesRegex(UsageError, 'Missing variable name.'):
            # Run the magic
            self._magic.document('-f output/docs/mockfileinput.json')

    def test_generate_doc_template(self):
        # Mock that the user inputs a file name
        self._magic.shell.ev = MagicMock(return_value="output/docs/mockfileinput.json")
        # Run the magic
        self._magic.document('ds')
        # Check that the user is asked for file input
        self._magic.shell.ev.assert_called_with('input("Enter filename where the documentation should be stored")')
        # Check that cell content is updated
        self._magic.shell.set_next_input.assert_called_with('%document -f output/docs/mockfileinput.json ds', replace=True)
        # Capture the display output
        captor = StringIO()
        print(*self._magic.display.call_args[0], file=captor, flush=True)
        print(captor.getvalue())
        self.assertEqual(expected_widgets, captor.getvalue())
