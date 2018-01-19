from tap_bronto.schemas import get_field_selector, is_selected, \
    CONTACT_SCHEMA
from tap_bronto.state import incorporate, save_state
from tap_bronto.stream import Stream
from funcy import project

from datetime import datetime, timedelta

import pytz
import singer
import socket
import suds

LOGGER = singer.get_logger()  # noqa


class ContactStream(Stream):

    TABLE = 'contact'
    KEY_PROPERTIES = ['id']
    SCHEMA = CONTACT_SCHEMA

    def make_filter(self, start, end):
        start_filter = self.client.factory.create('dateValue')
        start_filter.value = start
        start_filter.operator = 'AfterOrSameDay'

        end_filter = self.client.factory.create('dateValue')
        end_filter.value = end
        end_filter.operator = 'Before'

        _filter = self.client.factory.create('contactFilter')
        _filter.type = 'AND'
        _filter.modified = [start_filter, end_filter]

        return _filter

    def any_selected(self, field_names):
        sub_catalog = project(field_names, self.catalog.get('schema'))
        return any([is_selected(field_catalog)
                    for field_catalog in sub_catalog])

    def sync(self):
        key_properties = self.catalog.get('key_properties')
        table = self.TABLE

        singer.write_schema(
            self.catalog.get('stream'),
            self.catalog.get('schema'),
            key_properties=key_properties)

        self.login()

        field_selector = get_field_selector(
            self.catalog.get('schema'))

        includeGeoIpData = self.any_selected([
            'geoIPCity', 'geoIPStateRegion', 'geoIPZip',
            'geoIPCountry', 'geoIPCountryCode'
        ])

        includeTechnologyData = self.any_selected([
            'primaryBrowser', 'mobileBrowser', 'primaryEmailClient'
            'mobileEmailClient', 'operatingSystem'
        ])

        includeRFMData = self.any_selected([
            'firstOrderDate', 'lastOrderDate', 'lastOrderTotal'
            'totalOrders', 'totalRevenue', 'averageOrderValue'
        ])

        includeEngagementData = self.any_selected([
            'lastDeliveryDate', 'lastOpenDate', 'lastClickDate'
        ])

        if includeGeoIpData:
            LOGGER.info('Including GEOIP data.')

        if includeTechnologyData:
            LOGGER.info('Including technology data.')

        if includeRFMData:
            LOGGER.info('Including RFM data.')

        if includeEngagementData:
            LOGGER.info('Including engagement data.')

        LOGGER.info('Syncing contacts.')

        start = self.get_start_date(table)
        end = start
        interval = timedelta(hours=6)

        def flatten(item):
            read_only_data = item.get('readOnlyContactData', {})
            item.pop('readOnlyContactData', None)
            return {**item, **read_only_data}

        while end < datetime.now(pytz.utc):
            self.login()
            start = end
            end = start + interval
            LOGGER.info("Fetching contacts modified from {} to {}".format(
                start, end))

            _filter = self.make_filter(start, end)
            field_selector = get_field_selector(
                self.catalog.get('schema'))

            pageNumber = 1
            hasMore = True

            while hasMore:
                retry_count = 0

                self.login()

                try:
                    results = self.client.service.readContacts(
                        filter=_filter,
                        includeLists=True,
                        fields=[],
                        pageNumber=pageNumber,
                        includeSMSKeywords=True,
                        includeGeoIpData=includeGeoIpData,
                        includeTechnologyData=includeTechnologyData,
                        includeRFMData=includeRFMData,
                        includeEngagementData=includeEngagementData)

                except socket.timeout:
                    retry_count += 1
                    if retry_count >= 5:
                        LOGGER.error("Retried more than five times, moving on!")
                        raise
                    LOGGER.warn("Timeout caught, retrying request")
                    continue

                pageNumber = pageNumber + 1

                result_dicts = [suds.sudsobject.asdict(result)
                                for result in results]

                flattened = [flatten(result) for result in result_dicts]

                LOGGER.info("... {} results".format(len(flattened)))

                singer.write_records(
                    table,
                    [field_selector(result) for result in flattened])

                if len(results) == 0:
                    hasMore = False

            self.state = incorporate(
                self.state, table, 'modified',
                start.replace(microsecond=0).isoformat())

            save_state(self.state)

        LOGGER.info("Done syncing contacts.")
