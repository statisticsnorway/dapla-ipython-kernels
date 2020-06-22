import requests


class AbstractClient:
    def __init__(
            self,
            user_token,
            base_url,
    ):
        self._user_token = user_token
        self._base_url = base_url


class DataAccessClient(AbstractClient):

    def read_location(self, path):
        data_access_url = self._base_url + 'rpc/DataAccessService/readLocation'
        location_request = {"path": path, "snapshot": 0}
        response = requests.post(data_access_url, json=location_request,
                                 headers={
                                     'Authorization': 'Bearer %s' % self._user_token
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
