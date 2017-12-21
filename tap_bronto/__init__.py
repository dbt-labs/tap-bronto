import argparse
import json
import singer

from tap_bronto.endpoints.contact import ContactStream
from tap_bronto.endpoints.list import ListStream
from tap_bronto.endpoints.unsubscribe import UnsubscribeStream
from tap_bronto.endpoints.outbound_activity import OutboundActivityStream
from tap_bronto.endpoints.inbound_activity import InboundActivityStream

from tap_bronto.schemas import is_selected
from tap_bronto.state import load_state, save_state

LOGGER = singer.get_logger()  # noqa


def validate_config(config):
    required_keys = ['token']
    missing_keys = []
    null_keys = []
    has_errors = False

    for required_key in required_keys:
        if required_key not in config:
            missing_keys.append(required_key)

        elif config.get(required_key) is None:
            null_keys.append(required_key)

    if missing_keys:
        LOGGER.fatal("Config is missing keys: {}"
                     .format(", ".join(missing_keys)))
        has_errors = True

    if null_keys:
        LOGGER.fatal("Config has null keys: {}"
                     .format(", ".join(null_keys)))
        has_errors = True

    if has_errors:
        raise RuntimeError


def load_config(filename):
    config = {}

    try:
        with open(filename) as handle:
            config = json.load(handle)
    except Exception as e:
        LOGGER.info(e)
        LOGGER.fatal("Failed to decode config file. Is it valid json?")
        raise RuntimeError

    validate_config(config)

    return config


def load_catalog(filename):
    catalog = {}

    try:
        with open(filename) as handle:
            catalog = json.load(handle)
    except Exception:
        LOGGER.fatal("Failed to decode catalog file. Is it valid json?")
        raise RuntimeError

    return catalog


AVAILABLE_STREAM_ACCESSORS = [
    ContactStream,
    InboundActivityStream,
    ListStream,
    OutboundActivityStream,
    UnsubscribeStream,
]


def _is_selected(catalog_entry):
    default = catalog_entry.get('selected-by-default', False)

    return ((catalog_entry.get('inclusion') == 'automatic') or
            (catalog_entry.get('inclusion') == 'available' and
             catalog_entry.get('selected', default) is True))


def do_sync(args):
    LOGGER.info("Starting sync.")

    config = load_config(args.config)
    state = load_state(args.state)
    catalog = load_catalog(args.properties)

    stream_accessors = []

    for stream_catalog in catalog.get('streams'):
        stream_accessor = None

        if not is_selected(stream_catalog):
            LOGGER.info("'{}' is not marked selected, skipping."
                        .format(stream_catalog.get('stream')))
            continue

        for available_stream_accessor in AVAILABLE_STREAM_ACCESSORS:
            if available_stream_accessor.matches_catalog(stream_catalog):
                stream_accessors.append(available_stream_accessor(
                    config, state, stream_catalog))

                break

    for stream_accessor in stream_accessors:
        try:
            stream_accessor.state = state
            stream_accessor.sync()
            state = stream_accessor.state

        except Exception as exception:
            LOGGER.error(exception)
            LOGGER.error('Failed to sync endpoint, moving on!')

    save_state(state)


def do_discover(args):
    LOGGER.info("Starting discovery.")

    catalog = []

    config = load_config(args.config)

    for available_stream_accessor in AVAILABLE_STREAM_ACCESSORS:
        stream_accessor = available_stream_accessor(config)

        catalog += stream_accessor.generate_catalog()

    print(json.dumps({'streams': catalog}))


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')
    parser.add_argument(
        '-p', '--properties',
        help='Catalog file with fields selected')

    parser.add_argument(
        '-d', '--discover',
        help='Build a catalog from the underlying schema',
        action='store_true')
    parser.add_argument(
        '-S', '--select-all',
        help=('When "--discover" is set, this flag selects all '
              'fields for replication in the generated catalog'),
        action='store_true')

    args = parser.parse_args()

    try:
        if args.discover:
            do_discover(args)
        else:
            do_sync(args)

    except BaseException as exception:
        LOGGER.error(str(exception))
        LOGGER.fatal("Run failed.")
        exit(1)


if __name__ == '__main__':
    main()
