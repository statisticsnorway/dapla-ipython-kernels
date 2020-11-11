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

from ..jupyterextensions.authextension import AuthClient, AuthError
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
        return {
            'confidence': field['confidence'],
            'name': field['name'],
            'sources': field['selected'],
            'type': field['type']
        }

    return {'lineage': {
        'fields': list(map(mapper, lineage_json['lineage']['fields'])),
        'name': lineage_json['lineage']['name'],
        'sources': lineage_json['lineage']['sources'],
        'type': lineage_json['lineage']['type']
    }}


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
    def ensure_valid_filename(fname):
        if fname == '':
            raise UsageError("Invalid filename '{}'".format(fname))
        if not fname.endswith('.json'):
            fname = fname + ".json"
        return fname

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
        """This line magic assists creating documentation metadata for a given variable_name. The variable_name must
        be resolved to a Spark DataFrame.

        Usage:\\
          %lineage variable_name

        """
        ds, file_name, use_file_storage = self.get_input(line)

        try:
            if use_file_storage:
                file_exists = os.path.isfile(file_name)
                if file_exists:
                    with open(file_name, 'r') as f:
                        ds.lineage = json.load(f)
                else:
                    # Generate lineage from template
                    output_schema = {"schema": ds.schema.json(), "timestamp": self.current_milli_time()}
                    ds.lineage = self._lineage_template_provider(output_schema, self._declared_inputs)

                    with open(file_name, 'w', encoding="utf-8") as f:
                        json.dump(ds.lineage, f)
            else:
                # Generate lineage from template
                output_schema = {"schema": ds.schema.json(), "timestamp": self.current_milli_time()}
                ds.lineage = self._lineage_template_provider(output_schema, self._declared_inputs)
        except AuthError as err:
            err.print_warning()
            return

        accordion = self.create_widgets(ds.lineage)

        def on_button_clicked(b):
            with open(file_name, 'w', encoding="utf-8") as f:
                json.dump(ds.lineage, f)

        if use_file_storage:
            btn = widgets.Button(description='Save to file', icon='file-code')
            btn.on_click(on_button_clicked)
            out = widgets.Output()
            self.display(widgets.VBox([accordion, btn, out]))
        else:
            self.display(accordion)

    def get_input(self, line):
        opts, args = self.parse_options(line, 'f:', 'nofile')
        if not args:
            raise UsageError('Missing variable name.')
        fname = opts.get('f')
        if not fname:
            fname = 'lineage_{}.json'.format(args)  # add default json file if missing
        use_file_storage = 'nofile' not in opts
        self.validate_inputs()
        try:
            ds = self.shell.user_ns[args]
        except KeyError:
            raise UsageError("Could not find dataset '{}'".format(args))
        return ds, fname, use_file_storage

    def create_widgets(self, lineage):
        variable_titles = []
        variable_forms = []

        options_layout = widgets.Layout(
            overflow_y='auto',
            overflow_x='hidden',
            min_width='100px',
            max_width='300px',
            max_height='200px',
            flex_flow='column',
            display='flow-root',
            flex_shrink='0'
        )

        def capitalize_with_camelcase(s):
            return s[0].upper() + s[1:]

        for field in lineage['lineage']['fields']:
            variable_titles.append(capitalize_with_camelcase(field['name']))
            options = []
            for key, value in self._declared_inputs.items():
                options.append(widgets.Label(value=key))
                schema_fields = json.loads(value['schema'])['fields']

                primary_options = list(filter(lambda sf: sf['name'] == field['name'], schema_fields))
                additional_options = list(filter(lambda sf: sf['name'] != field['name'], schema_fields))

                def create_source_field(field_name):
                    # Find first match in sources with the given path (should only be one)
                    source = next(s for s in lineage['lineage']['sources'] if s['path'] == key)
                    return {
                        'field': field_name,
                        'path': source['path'],
                        'version': source['version']
                    }

                for schema_field in primary_options:
                    options.append(
                        self.create_checkbox(field, create_source_field(schema_field['name']), closest_match=True))
                if len(additional_options) > 0:
                    vbox = widgets.VBox(list(
                        map(lambda o: self.create_checkbox(field, create_source_field(o['name'])), additional_options)),
                        layout=options_layout)
                    options.append(vbox)

            options_widget = widgets.VBox(options)

            variable_forms.append(widgets.VBox([
                widgets.HTML('<style>.widget-checkbox-label-bold > label > span {font-weight: bold;}</style>'),
                widgets.HTML('Choose one or more sources for the variable <b>{}</b>. '
                             'Closest matching source variables are marked with <b>(*)</b>:'.format(field['name'])),
                options_widget]))
        accordion = widgets.Accordion(children=variable_forms)
        for i in range(len(variable_forms)):
            accordion.set_title(i, variable_titles[i])
        return accordion

    def create_checkbox(self, field, source_field, closest_match=False):
        checked = self.is_checked(field, source_field)

        w = widgets.Checkbox(
            description=source_field['field'],
            value=checked,
            style={'description_width': '0px'})

        if closest_match:
            w.description = w.description + ' (*)'
            w.add_class('widget-checkbox-label-bold')

        def on_value_change(change):
            if 'selected' not in field:
                field['selected'] = []
            if change['new']:
                field['selected'].append(source_field)
            else:
                field['selected'].remove(source_field)

        w.observe(on_value_change, names='value')
        return w

    def is_checked(self, field, source_field):
        if 'selected' in field:
            for selected in field['selected']:
                checked = selected['field'] == source_field['field'] and selected['path'] == source_field['path']
                if checked:
                    return True
        return False

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
