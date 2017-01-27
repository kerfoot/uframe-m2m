#!/usr/bin/env python

import os
import sys
import argparse
import logging
import json
import csv
from m2m.UFrameClient import UFrameClient

def main(args):
    '''Fetch all streams and all parameters produced by the fully-qualified reference designator'''

    # Translate the logging level string to numeric value
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(asctime)s:%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(level=log_level, format=log_format)
    
    # Environment
    # UFrame instance
    uframe_base_url = args.base_url
    if not uframe_base_url:
        uframe_base_url = os.getenv('UFRAME_BASE_URL')
        if not uframe_base_url:
            logging.error('No base_url set/found')
            return 1

    client = UFrameClient(uframe_base_url, timeout=args.timeout, args.direct)
    instruments = client.search_instruments(args.ref_des)
    if not instruments:
        return 0

    all_metadata = []
    for instrument in instruments:
        metadata = client.fetch_instrument_metadata(instrument)
        if not metadata:
            continue
        metadata['reference_designator'] = instrument

        all_metadata = metadata

    sys.stdout.write('{:s}\n'.format(json.dumps(metadata)))

    return 0

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('ref_des',
        type=str,
        help='Fully-qualified instrument reference designator')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        type=str,
        help='UFrame m2m base url beginning with https.  Taken from UFRAME_BASE_URL if not specified')
    arg_parser.add_argument('-t', '--timeout',
        type=int,
        default=120,
        help='Request timeout, in seconds <Default=120>')
    arg_parser.add_argument('-l', '--loglevel',
        help='Verbosity level <Default=warning>',
        type=str,
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='warning')
    arg_parser.add_argument('-d', '--direct',
        action='store_false',
        help='Send requests directly to UFrame, not via m2m (Not recommended)')


    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
