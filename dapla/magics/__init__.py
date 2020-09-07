from .documentation import DaplaDocumentationMagics


def load_all(ipython):
    from .documentation import load_ipython_extension
    load_ipython_extension(ipython)
