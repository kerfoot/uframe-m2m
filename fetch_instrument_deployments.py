#!/usr/bin/env python

import os
import sys
import argparse
import logging
import json
import csv
import datetime
from m2m.UFrameClient import UFrameClient


def main(args):
    '''Fetch all deployment events for the fully or partially qualified reference designator'''

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
        logging.debug('No instruments found ({:s})'.format(args.ref_des))
        return 0

    all_deployments = []
    for instrument in instruments:
        deployments = client.fetch_instrument_deployments(instrument)
        if not deployments:
            continue

        all_deployments = all_deployments + deployments

    if args.status == 'active':
        all_deployments = [d for d in all_deployments if not d['eventStopTime']]
    elif args.status == 'inactive':
        all_deployments = [d for d in all_deployments if d['eventStopTime']]

    for d in all_deployments:
        # Handle the inconsistent nature of the deployment asset management
        # schema
        if type(d['referenceDesignator']) == dict:
            d['ref_des'] = '-'.join([d['referenceDesignator']['subsite'],
                                     d['referenceDesignator']['node'],
                                     d['referenceDesignator']['sensor']])
        else:
            d['ref_des'] = d['referenceDesignator']

        # Create the event start timestamp
        try:
            d['eventStartTs'] = datetime.datetime.utcfromtimestamp(d['eventStartTime'] / 1000).strftime(
                '%Y-%m-%dT%H:%M:%SZ')
        except ValueError as e:
            logging.warning(e)
            d['eventStartTs'] = None

        # Create the event start timestamp
        d['eventStopTs'] = None
        if d['eventStopTime']:
            d['active'] = False
            try:
                d['eventStopTs'] = datetime.datetime.utcfromtimestamp(d['eventStopTime'] / 1000).strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
            except ValueError as e:
                logging.warning(e)
                d['eventStopTs'] = None
        else:
            d['active'] = True

    if args.csv:
        if not all_deployments:
            return 0

        csv_writer = csv.writer(sys.stdout)
        cols = ['ref_des',
                'eventStartTs',
                'eventStopTs',
                'eventStartTime',
                'eventStopTime',
                'deploymentNumber',
                'active']
        csv_writer.writerow(cols)
        for deployment in all_deployments:
            csv_writer.writerow([deployment[c] for c in cols])

    else:
        sys.stdout.write('{:s}\n'.format(json.dumps(all_deployments)))

    return 0


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('ref_des',
                            type=str,
                            help='Fully-qualified instrument reference designator')
    arg_parser.add_argument('-s', '--status',
                            dest='status',
                            type=str,
                            default='all',
                            choices=['active', 'inactive', 'all'],
                            help='Specify the status of the deployment <Default=all>')
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
