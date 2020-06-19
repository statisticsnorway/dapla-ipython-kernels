import os

"""
From https://github.com/jupyterhub/jupyter-server-proxy/issues/57
Fix the Spark UI link to point to the jupyter-server-proxy
"""

def uiWebUrl(self):
    from urllib.parse import urlparse
    web_url = self._jsc.sc().uiWebUrl().get()
    port = urlparse(web_url).port
    return os.environ['JUPYTERHUB_SERVICE_PREFIX'] + "proxy/{}/jobs/".format(port)
