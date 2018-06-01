from tap_bronto.schemas import with_properties, get_field_selector
from tap_bronto.state import incorporate, save_state
from tap_bronto.stream import Stream

from datetime import datetime, timedelta

import pytz
import singer
import zeep
from zeep.exceptions import Fault

LOGGER = singer.get_logger()  # noqa


class UnsubscribeStream(Stream):

    TABLE = 'unsubscribe'
    REPLICATION_KEY = 'created'
    KEY_PROPERTIES = ['contactId', 'method', 'created']
    SCHEMA, METADATA = with_properties({
        'contactId': {
            'type': ['string'],
            'description': ('The unique ID of the contact associated '
                            'with the unsubscribe.')
        },
        'method': {
            'type': ['string'],
            'description': ('The method used by the contact to '
                            'unsubscribe. The valid methods are: '
                            'subscriber, admin, bulk, listcleaning, '
                            'fbl (Feedback loop), complaint, '
                            'account, api')
        },
        'deliveryId': {
            'type': ['null', 'string'],
            'description': ('The unique ID of the delivery that '
                            'resulted in the contact unsubscribing.')
        },
        'complaint': {
            'type': ['null', 'string'],
            'description': ('Optional additional information about the '
                            'unsubscribe.')
        },
        'created': {
            'type': ['string'],
            'description': 'The date/time the unsubscribe was created.'
        }
    }, KEY_PROPERTIES, [REPLICATION_KEY])

    def make_filter(self, start, end):
        _filter = self.factory['unsubscribeFilter']
        return _filter(start=start, end=end)

    def sync(self):
        key_properties = self.catalog.get('key_properties')
        table = self.TABLE

        singer.write_schema(
            self.catalog.get('stream'),
            self.catalog.get('schema'),
            key_properties=key_properties)

        start = self.get_start_date(table)
        end = start
        interval = timedelta(hours=6)

        LOGGER.info('Syncing unsubscribes.')

        while end < datetime.now(pytz.utc):
            start = end
            end = start + interval
            LOGGER.info("Fetching unsubscribes from {} to {}".format(
                start, end))

            hasMore = True
            _filter = self.make_filter(start, end)
            pageNumber = 1

            field_selector = get_field_selector(self.catalog,
                self.catalog.get('schema'))

            while hasMore:
                LOGGER.info("... page {}".format(pageNumber))
                try:
                    results = self.client.service.readUnsubscribes(
                        filter=_filter, pageNumber=pageNumber)
                    pageNumber = pageNumber + 1

                    singer.write_records(
                        table,
                        [field_selector(zeep.helpers.serialize_object(result, target_cls=dict))
                          for result in results])
                except Fault as e:
                    if '103' in e.message:
                        LOGGER.warn("Got signed out - logging in again and retrying")
                        self.login()
                        continue
                    else:
                        raise

                LOGGER.info("... {} results".format(len(results)))

                if len(results) == 0:
                    hasMore = False

                self.state = incorporate(
                    self.state,
                    table,
                    self.REPLICATION_KEY,
                    start.isoformat())

                save_state(self.state)

        LOGGER.info("Done syncing unsubscribes.")
