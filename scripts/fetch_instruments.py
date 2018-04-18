#!/usr/bin/env python

import os
import sys
import argparse
import logging
import json
import csv
from m2m.UFrameClient import UFrameClient


def main(args):
    """Return all instruments registered on the UFrame system as fully qualified reference designators. Results are
    printed as valid JSON"""

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
        sys.stdout.write('{:s}\n'.format(json.dumps(instruments, sort_keys=True, indent=4)))

    return 0


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('ref_des',
                            nargs='?',
                            type=str,
                            help='Fully or partially-qualified reference designator to filter instruments')

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
                            default='info')

    arg_parser.add_argument('--csv',
                            help='Print results as csv records',
                            action='store_true')

    arg_parser.add_argument('-d', '--direct',
                            action='store_false',
                            help='Send requests directly to UFrame, not via m2m (Not recommended)')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
