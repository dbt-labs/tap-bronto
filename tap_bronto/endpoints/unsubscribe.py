from tap_bronto.schemas import with_properties, get_field_selector
from tap_bronto.state import incorporate, save_state
from tap_bronto.stream import Stream

from datetime import datetime, timedelta

import pytz
import singer
import suds

LOGGER = singer.get_logger()  # noqa


class UnsubscribeStream(Stream):

    TABLE = 'unsubscribe'
    KEY_PROPERTIES = ['contactId', 'method', 'created']
    SCHEMA = with_properties({
        'contactId': {
            'type': ['string'],
            'description': ('The unique ID of the contact associated '
                            'with the unsubscribe.'),
            'metadata': {
                'inclusion': 'automatic',
            },
        },
        'method': {
            'type': ['string'],
            'description': ('The method used by the contact to '
                            'unsubscribe. The valid methods are: '
                            'subscriber, admin, bulk, listcleaning, '
                            'fbl (Feedback loop), complaint, '
                            'account, api'),
            'metadata': {
                'inclusion': 'automatic',
            }
        },
        'deliveryId': {
            'type': ['null', 'string'],
            'description': ('The unique ID of the delivery that '
                            'resulted in the contact unsubscribing.'),
            'metadata': {
                'inclusion': 'available',
                'selected-by-default': False,
            },
        },
        'complaint': {
            'type': ['null', 'string'],
            'description': ('Optional additional information about the '
                            'unsubscribe.'),
            'metadata': {
                'inclusion': 'available',
                'selected-by-default': False,
            }
        },
        'created': {
            'type': ['string'],
            'description': 'The date/time the unsubscribe was created.',
            'metadata': {
                'inclusion': 'automatic',
            }
        }
    })

    def make_filter(self, start, end):
        _filter = self.client.factory.create('unsubscribeFilter')
        _filter.start = start
        _filter.end = end
        return _filter

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
            self.login()
            start = end
            end = start + interval
            LOGGER.info("Fetching unsubscribes from {} to {}".format(
                start, end))

            hasMore = True
            _filter = self.make_filter(start, end)
            pageNumber = 1

            field_selector = get_field_selector(
                self.catalog.get('schema'))

            while hasMore:
                self.login()
                LOGGER.info("... page {}".format(pageNumber))
                results = self.client.service.readUnsubscribes(
                    _filter, pageNumber)
                pageNumber = pageNumber + 1

                singer.write_records(
                    table,
                    [field_selector(suds.sudsobject.asdict(result))
                     for result in results])

                LOGGER.info("... {} results".format(len(results)))

                if len(results) == 0:
                    hasMore = False

                self.state = incorporate(
                    self.state,
                    table,
                    'start_date',
                    start.isoformat())

                save_state(self.state)

        LOGGER.info("Done syncing unsubscribes.")
