import singer
import suds
import sys

from tap_bronto.state import get_last_record_value_for_table
from dateutil import parser


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

    def get_start_date(self, table):
        LOGGER.info('Choosing start date for table {}'.format(table))
        default_start_string = self.config.get(
            'start_date',
            '2017-01-01T00:00:00-00:00')
        default_start = parser.parse(default_start_string)

        start = get_last_record_value_for_table(self.state, table)

        replication_method = self.catalog.get('replication_method',
                                              'INCREMENTAL')

        if replication_method == 'FULL_TABLE':
            LOGGER.info('Using FULL_TABLE replication. (All data since {})'
                        .format(default_start))
            start = default_start

        elif replication_method == 'INCREMENTAL' and start is None:
            LOGGER.info('Using INCREMENTAL replication, but no state entry '
                        'found. Performing full sync.  (All data since {})'
                        .format(default_start))
            start = default_start

        elif replication_method == 'INCREMENTAL' and start is not None:
            LOGGER.info('Using INCREMENTAL using last state entry. ({})'
                        .format(start))

        else:
            raise RuntimeError('Unknown replication method {}!'
                               .format(replication_method))
        return start

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
