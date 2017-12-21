from tap_bronto.schemas import get_field_selector, is_selected, \
    CONTACT_SCHEMA
from tap_bronto.stream import Stream
from funcy import project

import singer
import suds

LOGGER = singer.get_logger()  # noqa


class ContactStream(Stream):

    TABLE = 'contact'
    KEY_PROPERTIES = ['id']
    SCHEMA = CONTACT_SCHEMA

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

        hasMore = True
        pageNumber = 1
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

        LOGGER.info('Syncing contacts.')

        if includeGeoIpData:
            LOGGER.info('Including GEOIP data.')

        if includeTechnologyData:
            LOGGER.info('Including technology data.')

        if includeRFMData:
            LOGGER.info('Including RFM data.')

        if includeEngagementData:
            LOGGER.info('Including engagement data.')

        while hasMore:
            self.login()

            LOGGER.info("... page {}".format(pageNumber))
            results = self.client.service.readContacts(
                filter=1,
                includeLists=True,
                fields=[],
                pageNumber=pageNumber,
                includeSMSKeywords=True,
                includeGeoIpData=includeGeoIpData,
                includeTechnologyData=includeTechnologyData,
                includeRFMData=includeRFMData,
                includeEngagementData=includeEngagementData)

            pageNumber = pageNumber + 1

            result_dicts = [suds.sudsobject.asdict(result)
                            for result in results]

            def flatten(item):
                read_only_data = item.get('readOnlyContactData', {})
                item.pop('readOnlyContactData', None)
                return {**item, **read_only_data}

            flattened = [flatten(result) for result in result_dicts]

            singer.write_records(
                table,
                [field_selector(result) for result in flattened])

            LOGGER.info("... {} results".format(len(results)))

            #if len(results) == 0:
            hasMore = False

        LOGGER.info("Done syncing contacts.")
