import email
import imaplib
import os
from datetime import timedelta

from utils.logger import Logger


class EmailFetcher:
    def __init__(self, imap_server='imap.gmail.com', email_address=None, password=None):
        self.logger = Logger().get_logger()
        self.imap_server = imap_server
        self.email_address = email_address or os.getenv('EMAIL_ADDRESS')
        self.password = password or os.getenv('EMAIL_PASSWORD')
        self.mail = None

    def __enter__(self):
        """Connects to the IMAP server when used in a `with` statement."""
        self.mail = imaplib.IMAP4_SSL(self.imap_server)
        self.mail.login(self.email_address, self.password)
        self.mail.select('INBOX')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Logs out from the IMAP server when exiting the `with` statement."""
        self.mail.logout()

    def fetch_verification_code(self, execution_date, sender_email):
        if self.mail is None:
            self.logger.error("Error: Must be used in a 'with' statement to connect to the server")
            raise RuntimeError("Must be used in a 'with' statement to connect to the server")

        try:
            since_date = (execution_date - timedelta(days=1)).strftime("%d-%b-%Y")
            search_query = f'(FROM "{sender_email}" TEXT "verification code" SINCE {since_date})'
            self.logger.info(f"Searching for verification emails from '{sender_email}' since {since_date}")
            _, email_ids = self.mail.search(None, search_query)
            email_id_list = email_ids[0].split()

            if email_id_list:
                email_id = email_id_list[-1]
                self.logger.info(f"Fetching email with ID: {email_id}")
                _, msg_data = self.mail.fetch(email_id, '(RFC822)')
                raw_email = msg_data[0][1].decode("utf-8")
                msg = email.message_from_string(raw_email)
                subject = msg['Subject']
                verification_code = subject[0:6] if subject else None
                self.logger.info(f"Verification code found: {verification_code}")
                return verification_code
            else:
                self.logger.warning("No verification emails found.")
                return None
        except imaplib.IMAP4.error as e:
            self.logger.error(f"IMAP error: {e}")
            return None
        except Exception as e:  # Catching general exceptions for unexpected errors
            self.logger.error(f"Unexpected error during verification code fetch: {e}")
            return None
