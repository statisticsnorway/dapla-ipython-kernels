import os
import jwt
import time
from pyspark import SparkContext
from pyspark.sql import DataFrameReader, DataFrameWriter, SparkSession
from ..jupyterextensions.authextension import AuthClient, AuthError
from .decorators import add_lineage, add_lineage_option, add_doc_option, validate_documentation, validate_lineage

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


def load_extensions():
    DataFrameReader.path = namespace_read
    DataFrameWriter.path = namespace_write


@add_lineage
def namespace_read(self, ns):
    try:
        return get_session().read.format("gsim").load(ns)
    except AuthError as err:
        err.print_warning()


@validate_documentation
@validate_lineage
@add_doc_option
@add_lineage_option
def namespace_write(self, ns):
    try:
        self._spark = get_session()
        self.format("gsim").save(ns)
    except AuthError as err:
        err.print_warning()


def get_session():
    session = SparkSession._instantiatedSession
    if AuthClient.is_ready() and should_reload_token(session.sparkContext.getConf()):
        # Fetch new access token
        SparkContext._active_spark_context._conf.set("spark.ssb.access", AuthClient.get_access_token())
    return session


def should_reload_token(conf):
    spark_token = conf.get("spark.ssb.access")
    if spark_token is None:
        # First time fetching the token
        return True

    access_token = jwt.decode(spark_token, options={"verify_signature": False})
    diff_access = access_token['exp'] - time.time()
    # Should fetch new token from server if the access token within the given buffer
    if diff_access > int(os.environ['SPARK_USER_TOKEN_EXPIRY_BUFFER_SECS']):
        return False
    else:
        return True
