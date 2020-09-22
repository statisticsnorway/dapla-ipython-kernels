from __future__ import print_function
from IPython.core.magic import (Magics, magics_class, line_cell_magic, line_magic, cell_magic)
from IPython.core.error import UsageError, StdinNotImplementedError
import os
import time
import json
from pyspark.sql import DataFrame
import ipywidgets as widgets
from ..services.clients import DatasetDocClient
from ..jupyterextensions.authextension import AuthClient, AuthError


@magics_class
class DaplaLineageMagics(Magics):
    """Magics related to lineage management."""

    def __init__(self, shell, lineage_template_provider):
        # You must call the parent constructor
        super(DaplaLineageMagics, self).__init__(shell)
        self._lineage_template_provider = lineage_template_provider
        self._input_datasets = {}
        self._output_datasets = {}

    @staticmethod
    def display(*objs):
        from IPython.display import display
        display(*objs)

    @staticmethod
    def current_milli_time():
        return int(round(time.time() * 1000))

    @line_magic
    def lineage_input(self, line):
        """
        Define input datasets
        """
        path, schema = line.split(' ')
        if schema is None:
            # Referenced schema can be added later
            self._input_datasets[path] = {}
        else:
            self._input_datasets[path] = {"schema": schema, "timestamp": self.current_milli_time()}

    @line_magic
    def lineage_output(self, line):
        """
        Define output datasets
        """
        path, schema = line.split(' ')
        if schema is None:
            # Referenced schema can be added later
            self._output_datasets[path] = {}
        else:
            self._output_datasets[path] = {"schema": schema, "timestamp": self.current_milli_time()}

    @line_magic
    def lineage_tree(self, line):
        print(u'Input datasets:\n |-- {}'.format('\n |-- '.join(self._input_datasets.keys())))
        print(u'Output datasets:\n |-- {}'.format('\n |-- '.join(self._output_datasets.keys())))

    @line_magic
    def lineage_json(self, line):
        opts, args = self.parse_options(line, '', 'path')
        use_path = 'path' in opts
        if use_path:
            if args not in self._output_datasets.keys():
                raise UsageError('Could not find path {} in output datasets'.format(args))
            return self._lineage_template_provider(self._output_datasets[args], self._input_datasets)
        elif not args:
            raise UsageError('Missing dataset name.')
        else:
            ds = self.shell.user_ns[args]
            output_schema = {"schema": ds.schema.json(), "timestamp": self.current_milli_time()}
            return self._lineage_template_provider(output_schema, self._input_datasets)

    @line_magic
    def lineage_fields(self, line):
        opts, args = self.parse_options(line, '')
        if not args:
            raise UsageError('Missing dataset name.')
        try:
            ds = self.shell.user_ns[args]
            # Generate lineage from template
            output_schema = {"schema": ds.schema.json(), "timestamp": self.current_milli_time()}
            ds.lineage = self._lineage_template_provider(output_schema, self._input_datasets)
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
            for key, value in self._input_datasets.items():
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
    magics = DaplaLineageMagics(ipython, doc_template_client.get_doc_template)
    ipython.register_magics(magics)
