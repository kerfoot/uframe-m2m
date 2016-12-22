#!/usr/bin/env python

import os
import sys
import argparse
import logging
import json
import csv
from m2m.UFrameClient import UFrameClient

def main(args):
    '''Fetch all parameters in the streams produced by the fully-qualified 
    reference designator'''
    
    # Translate the logging level string to numeric value
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(asctime)s:%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(level=log_level, format=log_format)
    
    # Environment
    # UFrame instance
    uframe_base_url = args.base_url
    if not uframe_base_url:
        uframe_base_url = os.getenv('UFRAME_M2M_BASE_URL')
        if not uframe_base_url:
            logging.error('No base_url set/found')
            return 1

    client = UFrameClient(uframe_base_url, timeout=args.timeout)
    instruments = client.search_instruments(args.ref_des)
    if not instruments:
        return 0

    all_parameters = []
    for instrument in instruments:
        parameters = client.fetch_instrument_parameters(instrument)
        if not parameters:
            continue
        for p in parameters:
            p['reference_designator'] = instrument

        all_parameters = all_parameters + parameters

    if args.csv:
        if not all_parameters:
            return 0

        csv_writer = csv.writer(sys.stdout)
        cols = all_parameters[0].keys()
        csv_writer.writerow(cols)
        for parameter in all_parameters:
            csv_writer.writerow([parameter[c] for c in cols])

    else:
        sys.stdout.write('{:s}\n'.format(json.dumps(all_parameters)))

    return 0

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('ref_des',
        type=str,
        help='Fully or partially-qualified instrument reference designator')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        type=str,
        help='UFrame m2m base url beginning with https.  Taken from UFRAME_M2M_BASE_URL if not specified')
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
