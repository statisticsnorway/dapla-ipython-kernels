import os
import requests
import jwt
import time
import json
from pyspark import SparkContext
from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter, SparkSession
from jupyterhub.services.auth import HubAuth

"""
This extension will overload the spark session object (spark) with a method called ``path``.
That means, that the "normal" spark expressions:
>>> spark.read.format("gsim").load("/ns")
and
>>> ds.write.format("gsim").save("/ns")
can be replaced by
>>> spark.read.path("/ns")
and
>>> ds.write.path("/ns")
respectively. 

The ``path`` method will ensure that an access token is (re)loaded (if necessary) and added to the spark context.
"""


def load_extensions(read_hook= None, write_hook= None):
    DataFrameReader.path = namespace_read
    DataFrameWriter.path = namespace_write
    DataFrame.printDocTemplate = print_doc
    DataFrame.printAvroSchema = print_avro_schema


def print_doc(self, simple=False):
    doc_template = get_doc_template(self, simple)
    print(doc_template)
    if not simple:
        print("Use printDocTemplate(True) for a simplified template")


def print_avro_schema(self, record_name="spark_schema", record_namespace=""):
    avro_schema = self._sc._jvm.no.ssb.dapla.spark.plugin.SparkSchemaConverter.toAvroSchema(self._jdf.schema(),
                                                                                            record_name,
                                                                                            record_namespace)
    print(avro_schema.toString(True))


def namespace_read(self, ns):
    try:
        return get_session().read.format("gsim").load(ns)
    except AuthError as err:
        err.print_warning()


def namespace_write(self, ns):
    try:
        self._spark = get_session()
        # Read doc from parent dataframe
        if hasattr(self._df, 'doc'):
            doc = self._df.doc
            # doc can be either str or native json
            if type(doc) is str:
                self.format("gsim").option("dataset-doc", doc).save(ns)
            else:
                self.format("gsim").option("dataset-doc", json.dumps(doc, indent=2)).save(ns)
        else:
            self.format("gsim").save(ns)
    except AuthError as err:
        err.print_warning()


def get_doc_template(self, simple):
    use_simple = "true" if simple else "false"
    # Call Java class via jvm gateway
    return self._sc._jvm.no.ssb.dapla.spark.plugin.SparkSchemaConverter.toSchemaTemplate(self._jdf.schema(), use_simple)


def get_session():
    session = SparkSession._instantiatedSession
    if should_reload_token(session.sparkContext.getConf()):
        # Fetch new access token
        update_tokens()
    return session


def should_reload_token(conf):
    spark_token = conf.get("spark.ssb.access")
    if spark_token is None:
        # First time fetching the token
        return True

    access_token = jwt.decode(spark_token, verify=False)
    diff_access = access_token['exp'] - time.time()
    # Should fetch new token from server if the access token within the given buffer
    if diff_access > int(os.environ['SPARK_USER_TOKEN_EXPIRY_BUFFER_SECS']):
        return False
    else:
        return True


def update_tokens():
    # Helps getting the correct ssl configs
    hub = HubAuth()
    response = requests.get(os.environ['JUPYTERHUB_HANDLER_CUSTOM_AUTH_URL'],
                            headers={
                                'Authorization': 'token %s' % hub.api_token
                            }, cert=(hub.certfile, hub.keyfile), verify=hub.client_ca, allow_redirects=False)
    if response.status_code == 200:
        SparkContext._active_spark_context._conf.set("spark.ssb.access", response.json()['access_token'])
    else:
        raise AuthError


class AuthError(Exception):
    """This exception class is used when the communication with the custom auth handler fails.
    This is normally due to stale auth session."""

    def print_warning(self):
        from IPython.core.display import display, HTML
        display(HTML('Your session has timed out. Please <a href="/hub/login">log in</a> to continue.'))
