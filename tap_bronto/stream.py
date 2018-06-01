import singer
import zeep
import sys

from singer import metadata
from tap_bronto.state import get_last_record_value_for_table
from dateutil import parser
from zeep.exceptions import Fault

BRONTO_WSDL = 'https://api.bronto.com/v4?wsdl'
WSDL_NAMESPACE = 'http://api.bronto.com/v4'

LOGGER = singer.get_logger()  # noqa


class Stream:

    TABLE = None
    KEY_PROPERTIES = []
    SCHEMA = {}
    REPLICATION_KEY = None

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

        mdata = metadata.to_map(self.catalog.get('metadata', {}))
        replication_method = metadata.get(mdata, (), 'replication-method') or 'INCREMENTAL'

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
        LOGGER.info("Logging in")
        try:
            client = zeep.Client(BRONTO_WSDL)
            session_id = client.service.login(
                self.config.get('token'))

            factory = client.type_factory(WSDL_NAMESPACE)
            session_header = client.get_element("{%s}sessionHeader" % WSDL_NAMESPACE)
            client.set_default_soapheaders([session_header(sessionId=session_id)])
            self.client = client
            self.factory = factory

        except Fault:
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
            'metadata': cls.METADATA
        }]
