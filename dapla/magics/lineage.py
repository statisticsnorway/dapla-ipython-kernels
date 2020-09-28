from __future__ import print_function

import json
import os
import time
import warnings

import ipywidgets as widgets
from IPython import get_ipython
from IPython.core.error import UsageError
from IPython.core.magic import (Magics, magics_class, line_magic, cell_magic)
from pyspark.sql import DataFrame

from ..jupyterextensions.authextension import AuthClient
from ..services.clients import DatasetDocClient


def lineage_enabled():
    return get_ipython().find_line_magic(DaplaLineageMagics.lineage.__name__) is not None


def lineage_input(df, path, version):
    """Add lineage info to an input Spark DataFrame."""
    if not isinstance(df, DataFrame):
        raise UsageError("The variable '{}' is not a pyspark DataFrame".format(df))
    if lineage_enabled() and df is not None:
        get_ipython().run_line_magic(DaplaLineageMagics.on_input_load.__name__, "{} {} {}"
                                     .format(path, version, df.schema.json()))


def extract_lineage(df, path, version):
    """Extract lineage info from a given Spark DataFrame."""
    if not isinstance(df, DataFrame):
        raise UsageError("The variable '{}' is not a pyspark DataFrame".format(df))
    elif lineage_enabled():
        try:
            get_ipython().run_line_magic(DaplaLineageMagics.on_output_save.__name__, "{} {} {}"
                                         .format(path, version, df.schema.json()))
            if hasattr(df, 'lineage'):
                return map_lineage(df.lineage)
            # Generate simple lineage
            return get_ipython().run_line_magic(DaplaLineageMagics.lineage_json.__name__, "--path {}".format(path))
        except UsageError:
            # Just skip lineage generation
            return None


def map_lineage(lineage_json):
    def mapper(field):
        if 'selected' in field:
            selections = field['selected']

            def source_filter(source):
                return source['path'] in selections and source['field'] in selections[source['path']]
            return {
                'confidence': 0.99,
                'name': field['name'],
                'sources': list(filter(source_filter, field['sources'])),
                'type': field['type']
            }
        else:
            return {
                'confidence': field['confidence'],
                'name': field['name'],
                'sources': field['sources'],
                'type': field['type']
            }

    return {'lineage': {
        'fields': list(map(mapper, lineage_json['lineage']['fields'])),
        'name': lineage_json['lineage']['name'],
        'sources': lineage_json['lineage']['sources'],
        'type': lineage_json['lineage']['type']
        }
    }

@magics_class
class DaplaLineageMagics(Magics):
    """Magics related to lineage management."""

    def __init__(self, shell, lineage_template_provider):
        # You must call the parent constructor
        super(DaplaLineageMagics, self).__init__(shell)
        self._lineage_template_provider = lineage_template_provider
        self._declared_inputs = {}
        self._declared_outputs = {}
        self._input_trace = {}
        self._output_trace = {}

    @staticmethod
    def display(*objs):
        from IPython.display import display
        display(*objs)

    @staticmethod
    def current_milli_time():
        return int(round(time.time() * 1000))

    @cell_magic
    def input(self, line, cell):
        """
        Register input datasets
        """
        opts, args = self.parse_options(line, '', 'append')
        append = 'append' in opts
        if not append:
            # Reset previous state
            self._declared_inputs = {}
        for line in cell.strip().split('\n'):
            if line.strip().startswith('#'):
                continue
            self._declared_inputs[line] = {}

    @cell_magic
    def output(self, line, cell):
        """
        Register input datasets
        """
        opts, args = self.parse_options(line, '', 'append')
        append = 'append' in opts
        if not append:
            # Reset previous state
            self._declared_outputs = {}
        for line in cell.strip().split('\n'):
            if line.strip().startswith('#'):
                continue
            self._declared_outputs[line] = {}

    @line_magic
    def on_input_load(self, line):
        """
        Internal method. Called by spark decorators after an input dataframe has been loaded.
        """
        path, version, schema = line.split(' ')
        if schema is None:
            warnings.warn('Could not find schema for path {}'.format(path))
            self._input_trace[path] = {}
        else:
            self._input_trace[path] = {"schema": schema, "timestamp": version}
        if path not in self._declared_inputs:
            self.show_missing_declaration_warning(path, self.input.__name__)
        else:
            self._declared_inputs[path] = self._input_trace[path]

    @line_magic
    def on_output_save(self, line):
        """
        Internal method. Called by spark decorators after an output dataframe has been saved.
        """
        path, version, schema = line.split(' ')
        if schema is None:
            warnings.warn('Could not find schema for path {}'.format(path))
            self._output_trace[path] = {}
        else:
            self._output_trace[path] = {"schema": schema, "timestamp": version}
        if path not in self._declared_outputs:
            self.show_missing_declaration_warning(path, self.output.__name__)
        else:
            self._declared_outputs[path] = self._output_trace[path]

    @line_magic
    def lineage_tree(self, line):
        def input_details(ds):
            return '{} ({})'.format(ds[0], 'loaded' if 'schema' in ds[1] else 'not loaded')

        print(u'Input datasets:\n |-- {}'.format('\n |-- '.join(map(input_details, self._declared_inputs.items()))))
        print(u'Output datasets:\n |-- {}'.format('\n |-- '.join(self._declared_outputs.keys())))

    @line_magic
    def lineage_json(self, line):
        opts, args = self.parse_options(line, '', 'path')
        use_path = 'path' in opts
        self.validate_inputs()
        if use_path:
            if args not in self._declared_outputs.keys():
                raise UsageError('Could not find path {} in declared outputs'.format(args))
            return self._lineage_template_provider(self._declared_outputs[args], self._declared_inputs)
        elif not args:
            raise UsageError('Missing dataset name.')
        else:
            ds = self.shell.user_ns[args]
            output_schema = {"schema": ds.schema.json(), "timestamp": self.current_milli_time()}
            return self._lineage_template_provider(output_schema, self._declared_inputs)

    @line_magic
    def lineage(self, line):
        opts, args = self.parse_options(line, '')
        if not args:
            raise UsageError('Missing dataset name.')
        self.validate_inputs()
        try:
            ds = self.shell.user_ns[args]
            # Generate lineage from template
            output_schema = {"schema": ds.schema.json(), "timestamp": self.current_milli_time()}
            ds.lineage = self._lineage_template_provider(output_schema, self._declared_inputs)
        except KeyError:
            raise UsageError("Could not find dataset '{}'".format(args))

        variable_titles = []
        variable_forms = []

        options_layout = widgets.Layout(
            overflow='auto',
            min_width='100px',
            max_width='300px',
            max_height='300px',
            flex_flow='column',
            display='flex'
        )

        def capitalize_with_camelcase(s):
            return s[0].upper() + s[1:]

        for field in ds.lineage['lineage']['fields']:
            variable_titles.append(capitalize_with_camelcase(field['name']))
            options = []
            for key, value in self._declared_inputs.items():
                options.append(widgets.Label(value=key))
                for source_field in json.loads(value['schema'])['fields']:
                    options.append(self.create_checkbox(field, key, source_field))

            options_widget = widgets.VBox(options, layout=options_layout)

            variable_forms.append(widgets.VBox([
                widgets.HTML('<style>.widget-checkbox-label-bold > label > span {font-weight: bold;}</style>'),
                widgets.HTML('Choose one or more sources for the variable <b>{}</b>. '
                             'Closest matching source variables are marked with <b>(*)</b>:'.format(field['name'])),
                options_widget]))

        accordion = widgets.Accordion(children=variable_forms)

        for i in range(len(variable_forms)):
            accordion.set_title(i, variable_titles[i])

        self.display(accordion)

    def create_checkbox(self, field, key, source_field):
        closest_match = self.is_closest_match(field['sources'], key, source_field['name'])
        w = widgets.Checkbox(
            description=source_field['name'],
            value=False,
            style={'description_width': '0px'})

        if closest_match:
            w.description = w.description + ' (*)'
            w.add_class('widget-checkbox-label-bold')

        def on_value_change(change):
            if 'selected' not in field:
                field['selected'] = {}
            if key not in field['selected']:
                field['selected'][key] = []
            if change['new']:
                field['selected'][key].append(source_field['name'])
            else:
                field['selected'][key].remove(source_field['name'])
        w.observe(on_value_change, names='value')
        return w

    def show_missing_declaration_warning(self, path, method_ref):
        from IPython.core.display import HTML
        self.display(HTML("<b>Warning:</b> The path {} has not been declared. "
                          "To suppress this warning add the following to a cell in your notebook: "
                          "<pre><code style='background: hsl(220, 80%, 90%)'>%%{}\n{}</code></pre>"
                          .format(path, method_ref, path)))

    def validate_inputs(self):
        if len(self._declared_inputs) == 0:
            raise UsageError("Input datasets are not defined. "
                             "To declare input paths add the following to a cell in your notebook:\n%%{}\n<path>"
                             .format(self.input.__name__))
        for declared_input in self._declared_inputs.items():
            if 'schema' not in declared_input[1]:
                raise UsageError('The dataset with path {} has not been loaded'.format(declared_input[0]))

    @staticmethod
    def is_closest_match(sources, path, field):
        for source in sources:
            if source['field'] == field and source['path'] == path:
                return True
        return False


# In order to actually use these magics, you must register them with a
# running IPython.

def load_ipython_extension(ipython):
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """
    doc_template_client = DatasetDocClient(AuthClient.get_access_token, os.environ['DOC_TEMPLATE_URL'])
    # This class must be registered with a manually created instance,
    # since its constructor has different arguments from the default:
    magics = DaplaLineageMagics(ipython, doc_template_client.get_lineage_template)
    ipython.register_magics(magics)
