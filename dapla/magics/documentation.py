# This code can be put in any Python module, it does not require IPython
# itself to be running already.  It only creates the magics subclass but
# doesn't instantiate it yet.
from __future__ import print_function
from IPython.core.magic import (Magics, magics_class, line_magic)
from IPython.core.error import UsageError, StdinNotImplementedError
import os
import json
from pyspark.sql import DataFrame
import ipywidgets as widgets
from ..services.clients import DatasetDocClient
from ..jupyterextensions.authextension import AuthClient


@magics_class
class DaplaDocumentationMagics(Magics):
    """Magics related to documentation management (loading, saving, editing, ...)."""

    def __init__(self, shell, doc_template_provider):
        # You must call the parent constructor
        super(DaplaDocumentationMagics, self).__init__(shell)
        self._doc_template_provider = doc_template_provider

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
    def clear_output():
        from IPython.display import clear_output
        clear_output()

    @line_magic
    def document(self, line):
        """This line magic assists creating documentation metadata for a given variable name. The variable must be a Spark DataFrame.

        Usage:\\
          %document [options] variablename

        Options:

            -f <filename>: specify a file where the documentation is stored
            -n : Documentation is not stored to file
        """

        opts, args = self.parse_options(line, 'nf:')
        if not args:
            raise UsageError('Missing variable name.')

        fname = opts.get('f')
        use_file_storage = 'n' not in opts

        try:
            ds = self.shell.user_ns[args]
        except KeyError:
            raise UsageError("Could not find variable name '{}'".format(args))

        if not isinstance(ds, DataFrame):
            raise UsageError("The variable '{}' is not a pyspark DataFrame".format(args))

        if fname is None and use_file_storage:
            fname = self.ensure_valid_filename(
                self.shell.ev('input("Enter filename where the documentation should be stored")'))
            # Update the user input for future use
            contents = "%document -f {} {}".format(fname, line)
            self.shell.set_next_input(contents, replace=True)
        elif use_file_storage:
            fname = self.ensure_valid_filename(fname)

        if hasattr(ds, 'doc') and ds.doc is not None:
            try:
                ans = self.shell.ask_yes_no("Do you wish to overwrite existing documentation?", default='n')
            except StdinNotImplementedError:
                # skip execution if this note is run in batch mode
                ans = False

            if ans is False:
                self.clear_output()
                return

        self.clear_output()

        if use_file_storage:
            file_exists = os.path.isfile(fname)
            if file_exists:
                with open(fname, 'r') as f:
                    ds.doc = json.load(f)
            else:
                # Generate doc from template and prepare file
                ds.doc = self._doc_template_provider(ds.schema.json(), False)
                with open(fname, 'w', encoding="utf-8") as f:
                    json.dump(ds.doc, f)
        else:
            # Generate doc from template
            ds.doc = self._doc_template_provider(ds.schema.json(), False)

        variable_titles = []
        variable_forms = []

        form_item_layout = widgets.Layout(
            display='flex',
            flex_flow='row',
            justify_content='space-between'
        )

        for instanceVar in ds.doc['instanceVariables']:
            variable_titles.append(instanceVar['name'].capitalize())
            form_items = []

            for key in instanceVar.keys():
                if key == 'name':
                    continue
                form_items.append(
                    widgets.Box([widgets.Label(value=key.capitalize()), self.create_widget(instanceVar, key)],
                                layout=form_item_layout))

            variable_forms.append(widgets.Box(form_items, layout=widgets.Layout(
                display='flex',
                flex_flow='column',
                align_items='stretch',
                width='50%'
            )))

        accordion = widgets.Accordion(children=variable_forms)

        for i in range(len(variable_forms)):
            accordion.set_title(i, variable_titles[i])

        dataset_doc = widgets.Box([
            widgets.Box([widgets.Label(value='Name'), self.create_widget(ds.doc, 'name')], layout=form_item_layout),
            widgets.Box([widgets.Label(value='Description'), self.create_widget(ds.doc, 'description')],
                        layout=form_item_layout)
            ], layout=widgets.Layout(
                display='flex',
                flex_flow='column',
                align_items='stretch',
                width='50%'
            )
        )

        display_objs = (widgets.HTML("<b>Dataset metadata</b>"), dataset_doc,
                          widgets.HTML("<b>Instance variables</b>"), accordion)

        def on_button_clicked(b):
            with open(fname, 'w', encoding="utf-8") as f:
                json.dump(ds.doc, f)

        if use_file_storage:
            btn = widgets.Button(description='Save to file', icon='file-code')
            btn.on_click(on_button_clicked)
            out = widgets.Output()
            self.display(*display_objs, btn, out)
        else:
            self.display(*display_objs)

    def create_widget(self, binding, key):
        if isinstance(binding[key], str):
            return self.create_text_input(binding, key)
        elif isinstance(binding[key], bool):
            return self.create_checkbox_input(binding, key)
        elif isinstance(binding[key], dict) and binding[key].__contains__('enums'):
            return self.create_enum_selector(binding, key)
        elif isinstance(binding[key], dict) and binding[key].__contains__('candidates'):
            return self.create_candidate_selector(binding, key)
        else:
            raise UsageError("Invalid type '{}'".format(key))

    def create_text_input(self, binding, key):
        if key == 'description':
            component = widgets.Textarea()
        else:
            component = widgets.Text()

        component.value = binding[key]

        def on_change(v):
            binding[key] = v['new']

        component.observe(on_change, names='value')
        return component

    def create_checkbox_input(self, binding, key):
        component = widgets.Checkbox(indent=False)
        component.value = binding[key]

        def on_change(v):
            binding[key] = v['new']

        component.observe(on_change, names='value')
        return component

    def create_enum_selector(self, binding, key):
        component = widgets.Dropdown()
        component.options = binding[key]['enums']
        component.value = binding[key]['selected-enum']

        def on_change(v):
            binding[key]['selected-enum'] = v['new']

        component.observe(on_change, names='value')
        return component

    def create_candidate_selector(self, binding, key):
        component = widgets.Dropdown()
        component.options = list(map(lambda o: (o['name'], o['id']), binding[key]['candidates']))
        component.value = binding[key]['selected-id']

        def on_change(v):
            binding[key]['selected-id'] = v['new']

        component.observe(on_change, names='value')
        return component


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
    magics = DaplaDocumentationMagics(ipython, doc_template_client.get_doc_template)
    ipython.register_magics(magics)

