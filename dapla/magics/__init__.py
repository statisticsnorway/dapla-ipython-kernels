from .documentation import DaplaDocumentationMagics
from .lineage import DaplaLineageMagics


def load_all(ipython):
    from .documentation import load_ipython_extension
    load_ipython_extension(ipython)
    from .lineage import load_ipython_extension
    load_ipython_extension(ipython)
