#!/usr/bin/env python

import os
import sys
import argparse
import logging
import json
import csv
from m2m.UFrameClient import UFrameClient

def main(args):
    '''Fetch all streams produced by the fully-qualified reference designator'''
    
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

    client = UFrameClient(uframe_base_url, timeout=args.timeout, m2m=args.direct)
    instruments = client.search_instruments(args.ref_des)
    if not instruments:
        return 0

    all_streams = []
    for instrument in instruments:
        streams = client.fetch_instrument_streams(instrument)
        if not streams:
            continue
        for s in streams:
            s['reference_designator'] = instrument

        all_streams = all_streams + streams

    if args.csv:
        if not all_streams:
            return 0

        csv_writer = csv.writer(sys.stdout)
        cols = all_streams[0].keys()
        csv_writer.writerow(cols)
        for stream in all_streams:
            csv_writer.writerow([stream[c] for c in cols])

    else:
        sys.stdout.write('{:s}\n'.format(json.dumps(all_streams, indent=4)))

    return 0

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('ref_des',
        type=str,
        help='Fully or partially-qualified instrument reference designator')
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
    arg_parser.add_argument('--csv',
        help='Print results as csv records',
        action='store_true')
    arg_parser.add_argument('-d', '--direct',
        action='store_false',
        help='Send requests directly to UFrame, not via m2m (Not recommended)')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
