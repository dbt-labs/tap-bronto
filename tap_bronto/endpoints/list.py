from tap_bronto.schemas import with_properties, get_field_selector
from tap_bronto.stream import Stream

import singer
import suds

LOGGER = singer.get_logger()  # noqa


class ListStream(Stream):

    TABLE = 'list'
    KEY_PROPERTIES = ['id']
    SCHEMA = with_properties({
        'id': {
            'type': ['string'],
            'description': ('The unique id assigned to the list.'),
            'metadata': {
                'inclusion': 'automatic',
            },
        },
        'name': {
            'type': ['string'],
            'description': ('The internal name of the list.'),
            'metadata': {
                'inclusion': 'available',
                'selected-by-default': False,
            },
        },
        'label': {
            'type': ['string'],
            'description': ('The external (customer facing) name '
                            'of the list. '),
            'metadata': {
                'inclusion': 'available',
                'selected-by-default': False,
            }
        },
        'activeCount': {
            'type': ['null', 'integer'],
            'description': ('The number of active contacts of '
                            'currently on the list.'),
            'metadata': {
                'inclusion': 'available',
                'selected-by-default': False,
            }
        },
        'status': {
            'type': ['string'],
            'description': ('The status of the list. Valid values '
                            'are active, deleted, and tmp'),
            'metadata': {
                'inclusion': 'automatic',
            }
        }
    })

    def sync(self):
        key_properties = self.catalog.get('key_properties')
        table = self.TABLE

        singer.write_schema(
            self.catalog.get('stream'),
            self.catalog.get('schema'),
            key_properties=key_properties)

        self.login()

        hasMore = True
        pageNumber = 1
        field_selector = get_field_selector(
            self.catalog.get('schema'))

        LOGGER.info('Syncing lists.')

        while hasMore:
            self.login()

            LOGGER.info("... page {}".format(pageNumber))
            results = self.client.service.readLists(
                1,  # weird hack -- this just happens to work if we
                    # pass 1 as the filter. Other values like None
                    # did not work
                pageNumber,
                5000)

            LOGGER.info("... {} results".format(len(results)))

            pageNumber = pageNumber + 1

            singer.write_records(
                table,
                [field_selector(suds.sudsobject.asdict(result))
                 for result in results])

            if len(results) == 0:
                hasMore = False

        LOGGER.info("Done syncing lists.")
