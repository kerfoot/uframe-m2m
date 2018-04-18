#!/usr/bin/env python

import os
import sys
import argparse
import logging
import json
import csv
from m2m.UFrameClient import UFrameClient
# Disables SSL warnings
import requests.packages.urllib3

requests.packages.urllib3.disable_warnings()


def main(args):
    """Fetch all registered subsites from UFrame and print as valid JSON"""

    # Set up logging
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)

    # Environment
    # UFrame instance
    uframe_base_url = args.base_url or os.getenv('UFRAME_BASE_URL')
    if not uframe_base_url:
        logging.error('No base_url set/found')
        return 1

    client = UFrameClient(uframe_base_url, timeout=args.timeout, m2m=args.direct)
    if args.inventory == 'sensor':
        subsites = client.fetch_subsites()
    else:
        subsites = client.fetch_deployment_subsites()

    if args.subsite:
        subsites = [s for s in subsites if s.find(args.subsite) > -1]

    if args.csv:
        if not subsites:
            return 0

        csv_writer = csv.writer(sys.stdout)
        csv_writer.writerow(['subsite'])
        for subsite in subsites:
            csv_writer.writerow([subsite])

    else:
        sys.stdout.write('{:s}\n'.format(json.dumps(subsites, sort_keys=True, indent=4)))

    return 0


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('subsite',
                            nargs='?',
                            type=str,
                            help='subsite search string.  If specified, return subsites containing the string')

    arg_parser.add_argument('-i', '--inventory',
                            type=str,
                            choices=['sensor', 'deployment'],
                            default='sensor',
                            help='Registered inventory type')

    arg_parser.add_argument('-b', '--baseurl',
                            dest='base_url',
                            type=str,
                            help='UFrame base url beginning with http(s).  Taken from UFRAME_BASE_URL if not specified')

    arg_parser.add_argument('-t', '--timeout',
                            type=int,
                            default=30,
                            help='Request timeout, in seconds')

    arg_parser.add_argument('-l', '--loglevel',
                            help='Verbosity level',
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
