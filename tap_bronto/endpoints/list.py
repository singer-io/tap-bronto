from tap_bronto.schemas import with_properties, get_field_selector
from tap_bronto.stream import Stream

import singer
import zeep
from zeep.exceptions import Fault

LOGGER = singer.get_logger()  # noqa


class ListStream(Stream):

    TABLE = 'list'
    KEY_PROPERTIES = ['id']
    SCHEMA, METADATA = with_properties({
        'id': {
            'type': ['string'],
            'description': ('The unique id assigned to the list.')
        },
        'name': {
            'type': ['string'],
            'description': ('The internal name of the list.')
        },
        'label': {
            'type': ['string'],
            'description': ('The external (customer facing) name '
                            'of the list. ')
        },
        'activeCount': {
            'type': ['null', 'integer'],
            'description': ('The number of active contacts of '
                            'currently on the list.')
        },
        'status': {
            'type': ['string'],
            'description': ('The status of the list. Valid values '
                            'are active, deleted, and tmp')
        }
    }, KEY_PROPERTIES, [])

    def make_filter(self):
        _filter = self.factory['mailListFilter']
        return _filter()

    def sync(self):
        key_properties = self.catalog.get('key_properties')
        table = self.TABLE

        singer.write_schema(
            self.catalog.get('stream'),
            self.catalog.get('schema'),
            key_properties=key_properties)

        hasMore = True
        pageNumber = 1
        field_selector = get_field_selector(self.catalog,
            self.catalog.get('schema'))

        LOGGER.info('Syncing lists.')

        _filter = self.make_filter()

        while hasMore:

            LOGGER.info("... page {}".format(pageNumber))
            try:
                results = self.client.service.readLists(
                    filter=_filter,
                    pageNumber=pageNumber,
                    pageSize=5000)
            except Fault as e:
                if '103' in e.message:
                    LOGGER.warn("Got signed out - logging in again and retrying")
                    self.login()
                    continue
                else:
                    raise

            LOGGER.info("... {} results".format(len(results)))

            pageNumber = pageNumber + 1

            singer.write_records(
                table,
                [field_selector(zeep.helpers.serialize_object(result, target_cls=dict))
                 for result in results])

            if len(results) == 0:
                hasMore = False

        LOGGER.info("Done syncing lists.")
