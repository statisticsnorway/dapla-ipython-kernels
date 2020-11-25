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
from ..jupyterextensions.authextension import AuthClient, AuthError


def extract_doc(df):
    if not isinstance(df, DataFrame):
        raise UsageError("The variable '{}' is not a pyspark DataFrame".format(df))
    if hasattr(df, 'doc'):
        return map_doc_output(df.doc)
    else:
        return None


def map_doc_output(doc_json):
    def filter_ignored_variables(variable):
        # Todo - use an ignore field instead
        return variable['description'] is not None and variable['description'] != ''

    def map_variable(variable):
        return dict(map(map_variable_field, variable.items()))

    def map_variable_field(variable_field):
        if type(variable_field[1]) is dict:
            return variable_field[0], strip_content(variable_field[1])
        else:
            return variable_field

    def strip_content(field):
        names = ('candidates', 'enums')
        return dict(filter(lambda f: f[0] not in names, field.items()))

    return {
        'name': doc_json['name'],
        'description': doc_json['description'],
        'unitType': strip_content(doc_json['unitType']),
        'instanceVariables': list(map(map_variable, filter(filter_ignored_variables, doc_json['instanceVariables'])))}


@magics_class
class DaplaDocumentationMagics(Magics):
    """Magics related to documentation management (loading, saving, editing, ...)."""

    def __init__(self, shell, doc_template_provider, doc_template_candidates_provider):
        # You must call the parent constructor
        super(DaplaDocumentationMagics, self).__init__(shell)
        self._doc_template_provider = doc_template_provider
        self._doc_template_candidates_provider = doc_template_candidates_provider
        self._status = None
        self._result_status = ''

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
        """This line magic assists creating documentation metadata for a given variable_name. The variable_name must be
        resolved to a Spark DataFrame.

        Usage:\\
          %document [options] variable_name

        Options:

            --nofile  : Documentation is not stored to file
            -f <path> : Specifies a file path where the documentation is stored
        """
        self._result_status = ''  # clear status

        opts, args = self.parse_options(line, 'f:', 'nofile')
        if not args:
            raise UsageError('Missing variable name.')

        fname = opts.get('f')
        use_file_storage = 'nofile' not in opts

        try:
            ds = self.shell.user_ns[args]
        except KeyError:
            raise UsageError("Could not find variable name '{}'".format(args))

        if not isinstance(ds, DataFrame):
            raise UsageError("The variable '{}' is not a pyspark DataFrame".format(args))

        if fname is None and use_file_storage:
            fname = self.shell.ev('input("Enter filename where the documentation should be stored '
                                  '(leave blank for no file)")')
            # Update the user input for future use
            if fname == '':
                use_file_storage = False
                contents = "%document --nofile {}".format(line)
            else:
                contents = "%document -f {} {}".format(self.ensure_valid_filename(fname), line)
            self.shell.set_next_input(contents, replace=True)
        elif use_file_storage:
            fname = self.ensure_valid_filename(fname)

        if hasattr(ds, 'doc') and ds.doc is not None:
            try:
                ans = self.shell.ask_yes_no("The dataset '{}' has already been documented. "
                                            "Do you want to proceed?".format(args), default='n')
            except StdinNotImplementedError:
                # skip execution if this note is run in batch mode
                ans = False

            if ans is False:
                self.clear_output()
                return

        self.clear_output()

        try:
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
        except AuthError as err:
            err.print_warning()
            return

        variable_titles = []
        variable_forms = []

        form_item_layout = widgets.Layout(
            display='flex',
            flex_flow='row',
            justify_content='space-between'
        )

        def capitalize_with_camelcase(s):
            return s[0].upper() + s[1:]

        def create_dropdown_box(dict, title, key):
            inst_dropdown = self.create_widget(dict, key)
            label = widgets.Label(value=title)
            if self._status is None:
                dropdown = [label, inst_dropdown]
            else:
                dropdown = [label, inst_dropdown, widgets.HTML(self._status)]
                self._result_status = \
                    '<br/><i style="font-size:12px;color:red">' +\
                    'Types have been removed from Concept! Please check each instance variable</i>'
                self._status = None
            return widgets.Box(dropdown, layout=form_item_layout)

        for instanceVar in ds.doc['instanceVariables']:
            variable_titles.append(capitalize_with_camelcase(instanceVar['name']))
            form_items = []

            for key in instanceVar.keys():
                if key == 'name':
                    continue
                form_items.append(
                    create_dropdown_box(instanceVar, capitalize_with_camelcase(key), key)
                )

            variable_forms.append(widgets.Box(form_items, layout=widgets.Layout(
                display='flex',
                flex_flow='column',
                align_items='stretch',
                width='70%'
            )))

        accordion = widgets.Accordion(children=variable_forms, selected_index=None)

        for i in range(len(variable_forms)):
            accordion.set_title(i, variable_titles[i])

        dataset_doc = widgets.Box([
            create_dropdown_box(ds.doc, 'Name', 'name'),
            create_dropdown_box(ds.doc, 'Description', 'description'),
            create_dropdown_box(ds.doc, 'UnitType', 'unitType'),
        ], layout=widgets.Layout(
            display='flex',
            flex_flow='column',
            align_items='stretch',
            width='70%'
        )
        )

        display_objs = (widgets.HTML('<b style="font-size:14px">Dataset metadata</b>'), dataset_doc,
                        widgets.HTML('<b style="font-size:14px">Instance variables</b>{}'.format(self._result_status)), accordion)

        def on_button_clicked(b):
            with open(fname, 'w', encoding="utf-8") as f:
                json.dump(ds.doc, f)

        if use_file_storage:
            btn = widgets.Button(description='Save to file', icon='file-code')
            btn.on_click(on_button_clicked)
            out = widgets.Output()
            self.display(widgets.VBox([*display_objs, btn, out]))
        else:
            self.display(widgets.VBox(display_objs))

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
            raise UsageError("Unable to create a widget for '{}' with value '{}'\n{}"
                             .format(key, binding[key], json.dumps(binding, indent=2)))

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

    def check_selected_id(self, type, candidates, selected_id):
        if len(candidates) == 0:
            raise ValueError('candidates list was empty')
        for cand in candidates:
            if cand['id'] == selected_id:
                return selected_id
        # return first if selected id is not found
        first = candidates[0]['id']
        self._status = '{}" is removed! selecting:{}'.format(selected_id, first)
        return first

    def create_candidate_selector(self, binding, key):
        component = widgets.Dropdown()
        binding_key = binding[key]
        candidates = binding_key['candidates']
        selected_id = binding_key['selected-id']

        candidates_from_service = self._doc_template_candidates_provider(key)
        if len(candidates_from_service) > 0:
            candidates = candidates_from_service
            binding_key['candidates'] = candidates
            selected_id = self.check_selected_id(key, candidates, selected_id)
            binding_key['selected-id'] = selected_id

        component.options = list(map(lambda o: (o['name'], o['id']), candidates))
        component.value = selected_id

        def on_change(v):
            binding_key['selected-id'] = v['new']

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
    magics = DaplaDocumentationMagics(ipython, doc_template_client.get_doc_template,
                                      doc_template_client.get_doc_template_candidates)
    ipython.register_magics(magics)
