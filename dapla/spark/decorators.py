import json
from IPython import get_ipython


def add_lineage(read_method):
    def wrapper(self, ns):
        ds = read_method(self, ns)
        if ds is not None and lineage_enabled():
            get_ipython().run_line_magic("lineage_input", "{} {}".format(ns, ds.schema.json()))
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
    # Create a default dataset lineage or use df.lineage if present
    def wrap(self, ns):
        if hasattr(self._df, 'lineage'):
            # Use existing lineage info
            lineage = self._df.lineage
            if type(lineage) is str:
                self.option("dataset-lineage", lineage)
            else:
                self.option("dataset-lineage", json.dumps(lineage, indent=2))
        else:
            # Generate simple lineage
            get_ipython().run_line_magic("lineage_output", "{} {}".format(ns, self._df.schema.json()))
            lineage = get_ipython().run_line_magic("lineage_json", "--path {}".format(ns))
            print('Generated lineage: ' + lineage)
            if type(lineage) is str:
                self.option("dataset-lineage", lineage)
            else:
                self.option("dataset-lineage", json.dumps(lineage, indent=2))

        return write_method(self, ns)
    return wrap


def lineage_enabled():
    return get_ipython().find_line_magic("document") is not None