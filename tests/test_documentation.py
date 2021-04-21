import responses
import unittest
import json
from unittest.mock import MagicMock
from io import StringIO
from os.path import dirname

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from IPython.core.error import UsageError

from dapla.magics.documentation import DaplaDocumentationMagics, map_doc_output, remove_not_selected
from dapla.services.clients import DatasetDocClient
from tests import resolve_filename


class DaplaDocumentationMagicsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._spark = SparkSession.builder.config("spark.ui.enabled", False).getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls._spark.sparkContext.stop()

    def setUp(self):
        def candidates(a):
            if a == 'unitType':
                return [
                    {'id': 'UnitType_DUMMY', 'name': 'UnitType_DUMMY'}
                ]
            if a == 'selectionValue':
                return [
                    {"id": "id1", "name": "name1"},
                    {"id": "id2", "name": "name2"},
                    {"id": "id3", "name": "name3"}
                ]

        def enums(concept_type, enum_type):
            # if enumType == 'enums':
            return ["VAL1", "VAL2", "VAL3"]

        def translation(concept_type):
            if concept_type == "SelectionValue":
                return {
                    "name": "Seleksjon Verdi",
                    "description": "Beskrivelse av Seleksjon Verdi"
                }
            return {"name": concept_type}

        doc_template_client = DatasetDocClient(lambda: 'mock-user-token', 'http://mock.no/')
        self._magic = DaplaDocumentationMagics(
            None,
            doc_template_client.get_doc_template,
            candidates,
            enums,
            translation
        )
        self._magic.shell = MagicMock()
        self._magic.display = MagicMock()
        self._magic.get_dataset_path = lambda a1: ''

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
        responses.add(responses.GET, 'http://mock.no/doc/candidates/unitType?unitType',
                      json=[], status=200)
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
        responses.add(responses.GET, 'http://mock.no/doc/candidates/UnitType',
                      json=[], status=200)

        # Run the magic
        self._magic.document('--nofile ds')
        # Capture the display output
        captor = StringIO()
        print(*self._magic.display.call_args[0], file=captor, flush=True)
        print(captor.getvalue())
        self.assertEqual(expected_widgets, captor.getvalue())

    def test_lineage_output(self):
        with open(resolve_filename('doc_template.json'), 'r') as f:
            doc_template = json.load(f)
        with open(resolve_filename('doc_output.json'), 'r') as f:
            expected_doc = json.load(f)

        output = map_doc_output(doc_template)
        self.assertEqual(expected_doc, output)

    def test_check_and_remove_output(self):
        data = {
            "name": "ds name",
            "description": "ds description",
            'unitType': {'concept-type': 'UnitType', 'selected-id': 'UnitType_DUMMY', 'candidates': []},
            "instanceVariables": [
                {
                    "name": "iv1",
                    "description": "iv1descr",
                    "conceptExample": {
                        "concept-type": "ConceptExample",
                        "selected-id": "1",
                        "smart-match-id": "",
                        "candidates": []
                    },
                    "enumExample": {
                        "selected-enum": "VAL1",
                        "smart-enum": "",
                    }
                }
            ]
        }

        expected = {
            "name": "ds name",
            "description": "ds description",
            "unitType": {
                "concept-type": "UnitType",
                "selected-id": "UnitType_DUMMY"
            },
            "instanceVariables": [
                {
                    "name": "iv1",
                    "description": "iv1descr",
                    "conceptExample": {
                        "concept-type": "ConceptExample",
                        "selected-id": "1"
                    },
                    "enumExample": {
                        "selected-enum": "VAL1"
                    }
                }
            ]
        }

        doc_output = map_doc_output(data)
        output = remove_not_selected(doc_output)

        self.assertEqual(expected, output)

    def test_check_and_remove_not_assigned_instance_variables(self):
        data = {
            "name": "ds name",
            "description": "ds description",
            'unitType': {'concept-type': 'UnitType', 'selected-id': 'UnitType_DUMMY', 'candidates': []},
            "instanceVariables": [
                {
                    "name": "iv1",
                    "description": "iv1descr",
                    "conceptExample": {
                        "concept-type": "ConceptExample",
                        "selected-id": "please-select",
                        "smart-match-id": "",
                        "candidates": []
                    },
                    "enumExample": {
                        "selected-enum": "VAL1",
                        "smart-enum": "",
                    }
                }
            ]
        }

        expected = {
            "name": "ds name",
            "description": "ds description",
            "unitType": {
                "concept-type": "UnitType",
                "selected-id": "UnitType_DUMMY"
            },
            "instanceVariables": []
        }

        doc_output = map_doc_output(data)
        output = remove_not_selected(doc_output)

        self.assertEqual(expected, output)

    def test_remove_optional_not_assigned(self):
        data = {
            "name": "ds name",
            "description": "ds description",
            'unitType': {'concept-type': 'UnitType', 'selected-id': 'UnitType_DUMMY', 'candidates': []},
            "instanceVariables": [
                {
                    "name": "iv1",
                    "description": "iv1descr",
                    "conceptExample": {
                        "concept-type": "ConceptExample",
                        "selected-id": "id",
                        "smart-match-id": "",
                        "candidates": []
                    },
                    "optionalConcept": {
                        "concept-type": "optionalConcept",
                        "selected-id": "",
                        "optional": True,
                        "smart-match-id": "",
                        "candidates": []
                    },
                    "enumExample": {
                        "selected-enum": "VAL1",
                        "smart-enum": "",
                    },
                    "optionalEnum": {
                        "selected-enum": "please-select",
                        "optional": True,
                        "smart-enum": "",
                    }
                }
            ]
        }

        expected = {
            "name": "ds name",
            "description": "ds description",
            "unitType": {
                "concept-type": "UnitType",
                "selected-id": "UnitType_DUMMY"
            },
            "instanceVariables": [
                {
                    "name": "iv1",
                    "description": "iv1descr",
                    "conceptExample": {
                        "concept-type": "ConceptExample",
                        "selected-id": "id"
                    },
                    "enumExample": {
                        "selected-enum": "VAL1"
                    }
                }
            ]
        }

        doc_output = map_doc_output(data)
        output = remove_not_selected(doc_output)
        # print(json.dumps(output, indent=2))
        self.assertEqual(expected, output)

    def test_check_selected_id(self):
        candidates = [
            {
                'id': 'id-1',
                'name': 'Test 1'
            },
            {
                'id': 'id-2',
                'name': 'Test 2'
            }
        ]
        self.assertEqual('id-1', self._magic.check_selected_id('type', candidates, 'id-1'))
        self.assertEqual('id-2', self._magic.check_selected_id('type', candidates, 'id-2'))

        self.assertEqual('please-select', self._magic.check_selected_id('type', candidates, 'missing-id'))


doc_template = {
    "name": "ds name",
    "description": "ds description",
    'unitType': {'concept-type': 'UnitType',
                 'selected-id': 'UnitType_DUMMY',
                 'candidates': []
                 },
    "instanceVariables": [
        {
            "name": "iv1",
            "description": "iv1descr",
            "checkboxValue": False,
            "enumValue": {
                "selected-enum": "VAL2",
                "enums": []
            },
            "selectionValue": {
                "selected-id": "id3",
                "candidates": []
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
Box(children=(Label(value='Seleksjon Verdi'), \
Dropdown(index=2, options=(('name1', 'id1'), ('name2', 'id2'), ('name3', 'id3')), value='id3')), \
layout=Layout(display='flex', flex_flow='row', justify_content='space-between'))), \
layout=Layout(align_items='stretch', display='flex', flex_flow='column', width='70%')),), selected_index=None, \
_titles={'0': 'Iv1'})))\n"
