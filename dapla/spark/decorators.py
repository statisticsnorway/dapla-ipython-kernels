import json
import os

from ..magics.lineage import lineage_input, extract_lineage
from ..jupyterextensions.authextension import AuthClient
from ..services.clients import DataAccessClient


def add_lineage(read_method):
    def wrapper(self, ns):
        ds = read_method(self, ns)
        if '*' not in ns:
            data_access_client = DataAccessClient(AuthClient.get_access_token, os.environ['DATA_ACCESS_URL'])
            location_response = data_access_client.read_location(ns)
            lineage_input(ds, ns, location_response['version'])
        return ds
    return wrapper


def add_doc_option(write_method):
    def wrapper(self, ns):
        if hasattr(self._df, 'doc'):
            doc = self._df.doc
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
