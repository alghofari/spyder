import json

import bitwardentools

from utils.logger import Logger


class VaultwardenClient:
    def __init__(self, server: str, email: str, password: str, client_id: str, client_secret: str):
        self.logger = Logger().get_logger()

        self.server = server
        self.email = email
        self.password = password
        self.client_id = client_id,
        self.client_secret = client_secret

        self.bitwarden = bitwardentools.Client(server, email, password, authentication_cb=self._api_key)
        self._login()

    def _api_key(self, loginpayload):
        loginpayload.update(
            {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "api",
                "grant_type": "client_credentials"
            }
        )
        return loginpayload

    def _login(self):
        """Logs in to the Bitwarden instance and synchronizes the vault."""
        try:
            self.bitwarden.login()
            self.bitwarden.sync()
            self.logger.info('Successfully logged in and synced.')
        except bitwardentools.BitwardenError as bw_err:
            self.logger.error(f'Bitwarden login or sync failed: {bw_err}')
            raise
        except Exception as err:
            self.logger.error(f'An unexpected error occurred during login: {err}')
            raise

    def get_credentials_by_id(self, vault_id: str):
        """Retrieves the password associated with the given username from the vault."""
        try:
            items = self.bitwarden.search_objects({"id": f"{vault_id}"}, sync=True)
            for item in items:
                # Convert string to dictionary
                # Replace single quotes with double quotes and 'None' with 'null' for valid JSON format
                login_data = str(item.login).replace("'", '"').replace('None', 'null')

                # Parse the string into a Python dictionary
                login_data = json.loads(login_data)

                self.logger.info(f'Credentials found for vault id: {vault_id}')
                return login_data
        except bitwardentools.BitwardenError as bw_err:
            self.logger.error(f'Error retrieving ciphers: {bw_err}')
        except Exception as err:
            self.logger.error(f'An unexpected error occurred: {err}')
        return None
