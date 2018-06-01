from tap_bronto.schemas import get_field_selector, with_properties, ACTIVITY_SCHEMA
from tap_bronto.state import incorporate, save_state
from tap_bronto.stream import Stream
from zeep.exceptions import Fault

from datetime import datetime, timedelta

from funcy import identity, project, filter

import hashlib
import pytz
import singer
import zeep

LOGGER = singer.get_logger()  # noqa


class InboundActivityStream(Stream):

    TABLE = 'inbound_activity'
    REPLICATION_KEY = 'createdDate'
    KEY_PROPERTIES = ['id']
    SCHEMA, METADATA = with_properties(ACTIVITY_SCHEMA, KEY_PROPERTIES, [REPLICATION_KEY])

    def make_filter(self, start, end):
        _filter = self.factory['recentInboundActivitySearchRequest']

        return _filter(start=start, end=end, size=5000, readDirection='FIRST')

    def get_start_date(self, table):
        start = super().get_start_date(table)

        earliest_available = datetime.now(pytz.utc) - timedelta(days=30)

        if earliest_available > start:
            LOGGER.warn('Start date before 30 days ago, but Bronto '
                        'only returns the past 30 days of activity. '
                        'Using a start date of -30 days.')
            return earliest_available
        else:
            LOGGER.info('Rewinding three days, since activities can change...')

        return start - timedelta(days=3)

    def sync(self):
        key_properties = self.catalog.get('key_properties')
        table = self.TABLE

        singer.write_schema(
            self.catalog.get('stream'),
            self.catalog.get('schema'),
            key_properties=key_properties)

        start = self.get_start_date(table)
        end = start
        interval = timedelta(hours=1)

        LOGGER.info('Syncing inbound activities.')

        field_selector = get_field_selector(self.catalog,
                                            self.catalog.get('schema'))

        while end < datetime.now(pytz.utc):
            start = end
            end = start + interval
            LOGGER.info("Fetching activities from {} to {}".format(
                start, end))

            _filter = self.make_filter(start, end)
            hasMore = True

            while hasMore:
                try:
                    results = \
                        self.client.service.readRecentInboundActivities(
                            filter=_filter)
                except Fault as e:
                    if '116' in e.message:
                        hasMore = False
                        break
                    elif '103' in e.message:
                        LOGGER.warn("Got signed out - logging in again and retrying")
                        self.login()
                        continue
                    else:
                        raise

                result_dicts = [zeep.helpers.serialize_object(result, target_cls=dict)
                                for result in results]

                parsed_results = [field_selector(result)
                                  for result in result_dicts]

                for result in parsed_results:
                    ids = ['createdDate', 'activityType', 'contactId',
                           'listId', 'segmentId', 'keywordId', 'messageId']

                    result['id'] = hashlib.md5(
                        '|'.join(filter(identity,
                                        project(result, ids).values()))
                        .encode('utf-8')).hexdigest()

                singer.write_records(table, parsed_results)

                LOGGER.info('... {} results'.format(len(results)))

                _filter.readDirection = 'NEXT'

                if len(results) == 0:
                    hasMore = False

            self.state = incorporate(
                self.state, table, self.REPLICATION_KEY,
                start.replace(microsecond=0).isoformat())

            save_state(self.state)

        LOGGER.info('Done syncing inbound activities.')
