import requests
import json


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

    def write_dataset(self, path, version, valuation, state, parentUri, datasetMetaBytes, datasetMetaSignatureBytes):
        catalog_url = self._base_url + '/catalog/write'
        request = {
            "dataset": {
                "id": {
                    "path": path,
                    "timestamp": version
                },
                "valuation": valuation,
                "state": state,
                "parentUri": parentUri
            },
            "datasetMetaBytes": datasetMetaBytes,
            "datasetMetaSignatureBytes": datasetMetaSignatureBytes
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


def handle_error_codes(response):
    if response.status_code == 401:
        raise DataAccessError("Feil med tilgang til tjenesten", response.text)
    elif response.status_code == 404:
        raise DataAccessError("Fant ikke datasett", response.content)
    elif response.status_code < 200 | response.status_code >= 400:
        raise DataAccessError("En feil har oppstått: {}".format(response), response.text)


class DataAccessError(Exception):
    pass
