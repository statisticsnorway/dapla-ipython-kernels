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


def current_milli_time():
    return int(round(time.time() * 1000))


@magics_class
class DaplaLineageMagics(Magics):
    """Magics related to lineage management."""

    def __init__(self, shell, lineage_template_provider):
        # You must call the parent constructor
        super(DaplaLineageMagics, self).__init__(shell)
        self._lineage_template_provider = lineage_template_provider
        self._input_datasets = {}
        self._output_datasets = {}

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
            self._input_datasets[path] = {"schema": schema, "timestamp": current_milli_time()}

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
            self._output_datasets[path] = {"schema": schema, "timestamp": current_milli_time()}

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
            output_schema = {"schema": ds.schema.json(), "timestamp": current_milli_time()}
            return self._lineage_template_provider(output_schema, self._input_datasets)

    @line_magic
    def lineage_fields(self, line):
        opts, args = self.parse_options(line, '')
        if not args:
            raise UsageError('Missing dataset name.')
        try:
            ds = self.shell.user_ns[args]
            # Generate lineage from template
            output_schema = {"schema": ds.schema.json(), "timestamp": current_milli_time()}
            ds.lineage = self._lineage_template_provider(output_schema, self._input_datasets)
        except KeyError:
            raise UsageError("Could not find dataset '{}'".format(args))


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
