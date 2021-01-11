import requests
import requests_cache
import json

requests_cache.install_cache('client_cache', backend='memory', expire_after=2)

class AbstractClient:
    def __init__(
            self,
            user_token_provider,
            base_url,
    ):
        self._user_token_provider = user_token_provider
        self._base_url = base_url.rstrip('/')


class CatalogClient(AbstractClient):

    def list(self, path):
        catalog_url = self._base_url + '/rpc/CatalogService/listByPrefix'
        request = {"prefix": path}
        response = requests.post(catalog_url, json=request,
                                 headers={
                                     'Authorization': 'Bearer %s' % self._user_token_provider()
                                 }, allow_redirects=False)
        handle_error_codes(response)
        return response.json()

    def write_dataset(self, dataset_meta_all_bytes, dataset_meta_all_signature_bytes):
        catalog_url = self._base_url + '/catalog/write'
        request = {
            "datasetMetaAllBytes": dataset_meta_all_bytes,
            "datasetMetaAllSignatureBytes": dataset_meta_all_signature_bytes
        }
        response = requests.post(catalog_url, json=request,
                                 headers={
                                     'Authorization': 'Bearer %s' % self._user_token_provider()
                                 }, allow_redirects=False)
        handle_error_codes(response)


class DataAccessClient(AbstractClient):

    def read_location(self, path):
        data_access_url = self._base_url + '/rpc/DataAccessService/readLocation'
        location_request = {"path": path, "snapshot": 0}
        response = requests.post(data_access_url, json=location_request,
                                 headers={
                                     'Authorization': 'Bearer %s' % self._user_token_provider()
                                 }, allow_redirects=False)
        handle_error_codes(response)
        return response.json()

    def write_location(self, path, version, valuation, state):
        data_access_url = self._base_url + '/rpc/DataAccessService/writeLocation'
        metadata = {
            "id": {
                "path": path,
                "version": version
            },
            "valuation": valuation,
            "state": state
        }
        location_request = {
            "metadataJson": json.dumps(metadata)
        }
        response = requests.post(data_access_url, json=location_request,
                                 headers={
                                     'Authorization': 'Bearer %s' % self._user_token_provider()
                                 }, allow_redirects=False)
        handle_error_codes(response)
        return response.json()


class MetadataPublisherClient(AbstractClient):

    def __init__(
            self,
            user_token_provider,
            base_url,
            project_id,
            topic_name,
    ):
        super().__init__(user_token_provider, base_url)
        self._project_id = project_id
        self._topic_name = topic_name

    def data_changed(self, dataset_uri):
        publisher_url = self._base_url + '/rpc/MetadataDistributorService/dataChanged'
        if dataset_uri.startswith('/'):
            # Metadata publisher uses file scheme
            dataset_uri = 'file://' + dataset_uri
        request = {
            "projectId": self._project_id,
            "topicName": self._topic_name,
            "uri": dataset_uri
        }
        response = requests.post(publisher_url, json=request,
                                 headers={
                                     'Authorization': 'Bearer %s' % self._user_token_provider()
                                 }, allow_redirects=False)
        handle_error_codes(response)


class DatasetDocClient(AbstractClient):

    def get_doc_template(self, spark_schema, use_simple):
        request_url = self._base_url + '/doc/template'
        request = {
            "schema": spark_schema,
            "schemaType": "SPARK",
            "useSimpleFiltering": use_simple
        }
        response = requests.post(request_url, json=request,
                                 headers={
                                     'Authorization': 'Bearer %s' % self._user_token_provider()
                                 }, allow_redirects=False)
        handle_error_codes(response)
        return response.json()

    def map_type(self, type):
        map = {
            'unitType': 'UnitType',
            'representedVariable': 'RepresentedVariable',
            'population': 'Population',
            # This will make dataset-doc-service fetch both EnumeratedValueDomain and DescribedValueDomain
            'sentinelValueDomain': 'SentinelValueDomain'
        }
        if type in map:
            return map[type]
        return None

    def get_doc_template_candidates(self, type):
        concept_type = self.map_type(type)
        if concept_type is None:
            return ""

        request_url = self._base_url + '/doc/candidates/' + concept_type
        response = requests.get(request_url,
                                headers={
                                    'Authorization': 'Bearer %s' % self._user_token_provider()
                                }, allow_redirects=False)
        handle_error_codes(response)
        return response.json()

    def get_doc_validation(self, spark_schema, doc_template):
        request_url = self._base_url + '/doc/validate'
        request = {
            "dataDocTemplate": doc_template,
            "schema": spark_schema,
            "schemaType": "SPARK"
        }
        response = requests.post(request_url, json=request,
                                 headers={
                                     'Authorization': 'Bearer %s' % self._user_token_provider()
                                 }, allow_redirects=False)
        handle_error_codes(response)
        return response.json()

    def get_lineage_validation(self, spark_schema, doc_linage):
        request_url = self._base_url + '/lineage/validate'
        request = {
            "dataDocTemplate": doc_linage,
            "schema": spark_schema,
            "schemaType": "SPARK"
        }
        response = requests.post(request_url, json=request,
                                 headers={
                                     'Authorization': 'Bearer %s' % self._user_token_provider()
                                 }, allow_redirects=False)
        handle_error_codes(response)
        return response.json()

    def get_lineage_template(self, output_schema, input_schema_map, use_simple=False):
        request_url = self._base_url + '/lineage/template'

        def mapper(x):
            return (x[0], {
                "schema": x[1]['schema'],
                "schemaType": "SPARK",
                "timestamp": x[1]['timestamp'],
            })

        request = {
            "schema": output_schema['schema'],
            "timestamp": output_schema['timestamp'],
            "schemaType": "SPARK",
            "simpleLineage": use_simple,
            "dependencies": [dict(map(mapper, input_schema_map.items()))],
        }
        response = requests.post(request_url, json=request,
                                 headers={
                                     'Authorization': 'Bearer %s' % self._user_token_provider()
                                 }, allow_redirects=False)
        handle_error_codes(response)
        return response.json()

    def get_meta(self, path):
        request_url = self._base_url + '/doc/info?path=' + path
        response = requests.get(request_url,
                                headers={
                                    'Authorization': 'Bearer %s' % self._user_token_provider()
                                }, allow_redirects=False)
        handle_error_codes(response)
        return response.json()


def handle_error_codes(response):
    if response.status_code == 401:
        raise DataAccessError("Feil med tilgang til tjenesten", response.text)
    elif response.status_code == 404:
        raise DataAccessError("Fant ikke datasett", response.content)
    elif response.status_code < 200 | response.status_code >= 400:
        raise DataAccessError("En feil har oppstått: {}".format(response), response.text)


class DataAccessError(Exception):
    pass
