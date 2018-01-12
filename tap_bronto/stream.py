import singer
import suds
import sys

BRONTO_WSDL = 'https://api.bronto.com/v4?wsdl'

LOGGER = singer.get_logger()  # noqa


class Stream:

    TABLE = None
    KEY_PROPERTIES = []
    SCHEMA = {}

    def __init__(self, config={}, state={}, catalog=[]):
        self.client = None
        self.config = config
        self.state = state
        self.catalog = catalog

    def login(self):
        try:
            client = suds.client.Client(BRONTO_WSDL, timeout=3600)
            session_id = client.service.login(
                self.config.get('token'))
            session_header = client.factory.create('sessionHeader')
            session_header.sessionId = session_id
            client.set_options(soapheaders=session_header)
            self.client = client

        except suds.WebFault:
            LOGGER.fatal("Login failed!")
            sys.exit(1)

    @classmethod
    def matches_catalog(cls, catalog):
        return catalog.get('stream') == cls.TABLE

    def generate_catalog(self):
        cls = self.__class__

        return [{
            'tap_stream_id': cls.TABLE,
            'stream': cls.TABLE,
            'key_properties': cls.KEY_PROPERTIES,
            'schema': cls.SCHEMA,
            'metadata': {
                'selected-by-default': False,
                'inclusion': 'available',
            }
        }]
