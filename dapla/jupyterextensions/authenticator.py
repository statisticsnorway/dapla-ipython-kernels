"""
Custom GenericOAuthenticator extension
"""
from oauthenticator.generic import GenericOAuthenticator


# Pre-Spawn custom class to retrieve user access token
class EnvGenericOAuthenticator(GenericOAuthenticator):

    async def pre_spawn_start(self, user, spawner):

        self.log.info('Calling pre_spawn_start for: ' + user.name)
        # Retrieve user authentication info from JH
        auth_state = await user.get_auth_state()
        if not auth_state:
            # user has no auth state
            self.log.error('User has no auth state')
            return

        # update env var to pass to notebooks
        self.log.info('Starting notebook for: ' + user.name)

    # Refresh user access and refresh tokens (called periodically)
    async def refresh_user(self, user, handler, force=True):
        import jwt
        import time
        import urllib
        import json
        import os
        from tornado.httpclient import HTTPRequest, AsyncHTTPClient
        from tornado.httputil import url_concat
        # Retrieve user authentication info, decode, and check if refresh is needed
        auth_state = await user.get_auth_state()
        access_token = jwt.decode(auth_state['access_token'], verify=False)
        refresh_token = jwt.decode(auth_state['refresh_token'], verify=False)
        diff_access = access_token['exp']-time.time()
        diff_refresh = refresh_token['exp']-time.time()
        # Allow SPARK_USER_TOKEN_EXPIRY_BUFFER_SECS before expiry
        if diff_access > int(os.environ['SPARK_USER_TOKEN_EXPIRY_BUFFER_SECS']):
            # Access token still valid, function returns True
            refresh_user_return = True
        elif diff_refresh < 0:
            # Refresh token not valid, need to completely reauthenticate
            self.log.info('Refresh token not valid, need to completely reauthenticate for: ' +  user.name)
            refresh_user_return = None
        else:
            # We need to refresh access token (which will also refresh the refresh token)
            self.log.info('Try to refresh user tokens for: ' + user.name)
            refresh_token = auth_state['refresh_token']
            http_client = AsyncHTTPClient()
            url = os.environ.get('OAUTH2_TOKEN_URL')
            params = dict(
                grant_type = 'refresh_token',
                client_id = os.environ.get('OAUTH_CLIENT_ID'),
                client_secret = os.environ.get('OAUTH_CLIENT_SECRET'),
                refresh_token = refresh_token
            )

            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }

            req = HTTPRequest(url,
                              method="POST",
                              headers=headers,
                              validate_cert=self.tls_verify,
                              body=urllib.parse.urlencode(params)  # Body is required for a POST...
                              )
            resp = await http_client.fetch(req)

            resp_json = json.loads(resp.body.decode('utf8', 'replace'))

            access_token = resp_json['access_token']
            refresh_token = resp_json.get('refresh_token', None)
            token_type = resp_json['token_type']
            scope = resp_json.get('scope', '')
            if (isinstance(scope, str)):
                scope = scope.split(' ')

            # Determine who the logged in user is
            headers = {
                "Accept": "application/json",
                "User-Agent": "JupyterHub",
                "Authorization": "{} {}".format(token_type, access_token)
            }
            if self.userdata_url:
                url = url_concat(self.userdata_url, self.userdata_params)
            else:
                raise ValueError("Please set the OAUTH2_USERDATA_URL environment variable")

            if self.userdata_token_method == "url":
                url = url_concat(self.userdata_url, dict(access_token=access_token))

            req = HTTPRequest(url,
                              method=self.userdata_method,
                              headers=headers,
                              validate_cert=self.tls_verify,
                              )
            resp = await http_client.fetch(req)
            resp_json = json.loads(resp.body.decode('utf8', 'replace'))

            if not resp_json.get(self.username_key):
                self.log.error("OAuth user contains no key %s: %s", self.username_key, resp_json)
                return

            self.log.info('Updating access and refresh token for: ' + user.name)
            refresh_user_return = {
                'name': resp_json.get(self.username_key),
                'auth_state': {
                    'access_token': access_token,
                    'refresh_token': refresh_token,
                    'oauth_user': resp_json,
                    'scope': scope,
                }
            }
        return refresh_user_return
