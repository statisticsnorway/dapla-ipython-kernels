from tornado import gen, web
from jupyterhub.handlers import BaseHandler

"""
A custom request handler for JupyterHub. This handler returns user and auth state info
"""
class AuthHandler(BaseHandler):

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
            "username" : user.name,
            "access_token" : auth_state['access_token'],
            "refresh_token" : auth_state['refresh_token'],
        })

