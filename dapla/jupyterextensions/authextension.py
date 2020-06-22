import os
import requests
from tornado import web
from jupyterhub.handlers import BaseHandler
from jupyterhub.services.auth import HubAuth


class AuthHandler(BaseHandler):
    """
    A custom request handler for JupyterHub. This handler returns user and auth state info
    """

    @web.authenticated
    async def get(self):
        user = await self.get_current_user()
        if user is None:
            self.log.info('User is none')
            # whoami can be accessed via oauth token
            user = self.get_current_user_oauth_token()
        if user is None:
            raise web.HTTPError(403)

        self.log.info('User is ' + user.name)
        auth_state = await user.get_auth_state()
        if not auth_state:
            # user has no auth state
            self.log.error('User has no auth state')
            return

        self.write({
            "username": user.name,
            "access_token": auth_state['access_token'],
            "refresh_token": auth_state['refresh_token'],
        })


class AuthClient:
    """
    A client class that connects to the AuthHandler to retrieve user and auth state info
    """
    @staticmethod
    def get_access_token():
        # Helps getting the correct ssl configs
        hub = HubAuth()
        response = requests.get(os.environ['JUPYTERHUB_HANDLER_CUSTOM_AUTH_URL'],
                                headers={
                                    'Authorization': 'token %s' % hub.api_token
                                }, cert=(hub.certfile, hub.keyfile), verify=hub.client_ca, allow_redirects=False)
        if response.status_code == 200:
            return response.json()['access_token']
        else:
            raise AuthError


class AuthError(Exception):
    """This exception class is used when the communication with the custom auth handler fails.
    This is normally due to stale auth session."""

    def print_warning(self):
        from IPython.core.display import display, HTML
        display(HTML('Your session has timed out. Please <a href="/hub/login">log in</a> to continue.'))
