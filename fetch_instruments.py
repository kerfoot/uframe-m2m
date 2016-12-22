#!/usr/bin/env python

import os
import sys
import argparse
import logging
import json
import csv
from m2m.UFrameClient import UFrameClient

def main(args):
    '''Return all instruments registered on on the UFrame system as fully qualified reference designators'''
    
    # Set up the erddapfoo.lib.m2m.M2mClient logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)
    
    # Environment
    # UFrame instance
    uframe_base_url = args.base_url
    if not uframe_base_url:
        uframe_base_url = os.getenv('UFRAME_M2M_BASE_URL')
        if not uframe_base_url:
            logging.error('No base_url set/found')
            return 1

    client = UFrameClient(uframe_base_url, timeout=args.timeout)
    if args.ref_des:
        instruments = client.search_instruments(args.ref_des)
    else:
        instruments = client.instruments

    if args.csv:
        if not instruments:
            return 0

        csv_writer = csv.writer(sys.stdout)
        csv_writer.writerow(['reference_designator'])
        for instrument in instruments:
            csv_writer.writerow([instrument])

    else:
        sys.stdout.write('{:s}\n'.format(json.dumps(instruments)))

    return 0

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('ref_des',
        nargs='?',
        type=str,
        help='Fully or partially-qualified reference designator. If specified returns subsites that contain the reference designator')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        type=str,
        help='UFrame m2m base url beginning with https.  Taken from UFRAME_M2M_BASE_URL if not specified')
    arg_parser.add_argument('-t', '--timeout',
        type=int,
        default=120,
        help='Request timeout, in seconds <Default=120>')
    arg_parser.add_argument('-l', '--loglevel',
        help='Verbosity level <Default=info>',
        type=str,
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info')
    arg_parser.add_argument('--csv',
        help='Print results as csv records',
        action='store_true')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
