import json
import os

from ..magics.lineage import lineage_input, extract_lineage
from ..magics.documentation import extract_doc
from ..jupyterextensions.authextension import AuthClient
from ..services.clients import DataAccessClient
from ..services.clients import DatasetDocClient
from IPython.core.error import UsageError


def add_lineage(read_method):
    def wrapper(self, ns):
        ds = read_method(self, ns)
        if '*' not in ns:
            data_access_client = DataAccessClient(AuthClient.get_access_token, os.environ['DATA_ACCESS_URL'])
            location_response = data_access_client.read_location(ns)
            lineage_input(ds, ns, location_response['version'])
        return ds
    return wrapper


def validate_lineage(write_method):
    def wrapper(self, ns):
        data_doc_client = DatasetDocClient(AuthClient.get_access_token, os.environ['DOC_TEMPLATE_URL'])
        version = _current_milli_time()
        if not hasattr(self._df, 'lineage'):
            return write_method(self, ns)
        lineage = json.dumps(extract_lineage(self._df, ns, version), indent=2)
        schema = self._df.schema.json()
        validation = data_doc_client.get_lineage_validation(schema, lineage)
        status = validation['status']
        if status == 'ok':
            return write_method(self, ns)
        message = validation['message']
        raise UsageError("{}".format(message))
    return wrapper


def validate_documentation(write_method):
    def wrapper(self, ns):
        data_doc_client = DatasetDocClient(AuthClient.get_access_token, os.environ['DOC_TEMPLATE_URL'])
        doc = extract_doc(self._df)
        if doc is None:
            return write_method(self, ns)
        template_doc = json.dumps(doc, indent=2)
        schema = self._df.schema.json()
        validation = data_doc_client.get_doc_validation(schema, template_doc)
        status = validation['status']
        if status == 'ok':
            return write_method(self, ns)
        message = validation['message']
        raise UsageError("{}".format(message))
    return wrapper


def add_doc_option(write_method):
    def wrapper(self, ns):
        doc = extract_doc(self._df)
        if doc is not None:
            # doc can be either str or native json
            if type(doc) is str:
                self.option("dataset-doc", doc)
            else:
                self.option("dataset-doc", json.dumps(doc, indent=2))
        return write_method(self, ns)
    return wrapper


def add_lineage_option(write_method):
    def wrap(self, ns):
        # Create a default dataset lineage or use df.lineage if present
        version = _current_milli_time()
        lineage = extract_lineage(self._df, ns, version)
        if lineage is not None:
            self.option("version", version)
            if type(lineage) is str:
                self.option("lineage-doc", lineage)
            else:
                self.option("lineage-doc", json.dumps(lineage, indent=2))
        write_method(self, ns)
    return wrap


def _current_milli_time():
    import time
    return int(round(time.time() * 1000))
