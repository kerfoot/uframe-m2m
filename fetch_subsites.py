#!/usr/bin/env python

import os
import sys
import argparse
import logging
import json
import csv
from m2m.UFrameClient import UFrameClient

def main(args):
    '''Fetch all registered subsites from the /sensor/inv API endpoint'''
    
    # Set up the erddapfoo.lib.m2m.M2mClient logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)
    
    # Environment
    # UFrame instance
    uframe_base_url = args.base_url
    if not uframe_base_url:
        uframe_base_url = os.getenv('UFRAME_BASE_URL')
        if not uframe_base_url:
            logging.error('No base_url set/found')
            return 1

    client = UFrameClient(uframe_base_url, timeout=args.timeout)
    if args.inventory == 'sensor':
        subsites = client.fetch_subsites()
    else:
        subsites = client.fetch_deployment_subsites()

    if args.subsite:
        subsites = [s for s in subsites if s.find(args.subsite) > -1 ]

    if args.csv:
        if not subsites:
            return 0

        csv_writer = csv.writer(sys.stdout)
        csv_writer.writerow(['subsite'])
        for subsite in subsites:
            csv_writer.writerow([subsite])

    else:
        sys.stdout.write('{:s}\n'.format(json.dumps(subsites)))

    return 0

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('subsite',
        nargs='?',
        type=str,
        help='subsite search string.  If specified, return subsites containing the string')
    arg_parser.add_argument('-i', '--inventory',
        type=str,
        choices=['sensor', 'deployment'],
        default='sensor',
        help='Registered inventory type <Default=sensor>')
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

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
