#!/usr/bin/env python

import os
import sys
import argparse
import logging
import json
import csv
import datetime
from m2m.UFrameClient import UFrameClient
import pytz
from dateutil import parser
from collections import OrderedDict


def main(args):
    """Show the deployment status and stream particle overlap for currently deployed instruments"""

    # Set up logging
    logger = logging.getLogger(__name__)
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)

    # Environment
    # UFrame instance
    uframe_base_url = args.base_url or os.getenv('UFRAME_BASE_URL')
    if not uframe_base_url:
        logging.error('No base_url set/found')
        return 1

    client = UFrameClient(uframe_base_url, timeout=args.timeout, m2m=args.m2m)

    ref_des = args.ref_des
    if not ref_des:
        instruments = client.instruments
    else:
        instruments = client.search_instruments(args.ref_des)

    deployment_status = {'uframe': client.base_url, 'deployments': []}
    now = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
    for instrument in instruments:

        # Find all fully qualified reference designators
        all_deployments = client.fetch_instrument_deployments(instrument)
        if client.last_status_code != 200:
            continue

        if not all_deployments:
            logger.debug('No deployments found for instrument {:s}'.format(instrument))
            continue

        # Filter deployments based on deployment status
        all_deployments = client.filter_deployments_by_status(all_deployments, args.status)

        if not all_deployments:
            if args.status != 'all':
                logger.debug('No {:s} deployments found for instrument {:s}'.format(args.status, instrument))
                continue

        streams = client.fetch_instrument_streams(instrument)
        if not streams:
            logger.warning('No streams found for deployed instrument')
            continue

        for d in all_deployments:

            # Handle the inconsistent nature of the deployment asset management schema
            if type(d['referenceDesignator']) == dict:
                d['ref_des'] = '-'.join([d['referenceDesignator']['subsite'],
                                         d['referenceDesignator']['node'],
                                         d['referenceDesignator']['sensor']])
            else:
                d['ref_des'] = d['referenceDesignator']

            # Deployment event must have a parseable start time
            if not d['eventStartTime']:
                logger.warning('Deployment event has no eventStartTime')
                continue

            # Parse eventStartTime
            try:
                dt0 = datetime.datetime.utcfromtimestamp(d['eventStartTime'] / 1000).replace(tzinfo=pytz.UTC)
            except ValueError as e:
                logging.error(e)
                continue
            # Parse eventStopTime if there is one
            # Create the event stop timestamp
            active_status = True
            dt1 = None
            if d['eventStopTime']:
                try:
                    dt1 = datetime.datetime.utcfromtimestamp(d['eventStopTime'] / 1000).replace(tzinfo=pytz.UTC)
                    if dt1 < now:
                        active_status = False
                    d['eventStopTs'] = dt1.strftime('%Y-%m-%dT%H:%M:%SZ')
                except ValueError as e:
                    logging.warning(e)
                    dt1 = None

            # Loop through each stream
            for stream in streams:

                if args.telemetry and stream['method'].find(args.telemetry) == -1:
                    continue

                status = OrderedDict()
                status['reference_designator'] = d['ref_des']
                status['stream'] = stream['stream']
                status['telemetry'] = stream['method']
                status['deployment_number'] = d['deploymentNumber']
                status['active'] = False
                status['deployment_has_particles'] = True
                status['deployment_start_time'] = None
                status['deployment_end_time'] = None
                status['stream_start_time'] = stream['beginTime']
                status['stream_end_time'] = stream['endTime']
                status['stream_particle_count'] = stream['count']
                status['active'] = active_status

                # Parse the stream beginTime
                try:
                    st0 = parser.parse(stream['beginTime'])
                except ValueError as e:
                    logger.error(
                        '{:s} beginTime parse error - {:s} ({:s})'.format(stream['stream'], e, stream['endTime']))
                    continue

                # Parse the stream beginTime
                try:
                    st1 = parser.parse(stream['endTime'])
                except ValueError as e:
                    logger.error(
                        '{:s} endTime parse error - {:s} ({:s})'.format(stream['stream'], e, stream['endTime']))
                    continue

                # Check stream endTime to make sure it's not before the deployment began
                if st1 < dt0:
                    status['deployment_has_particles'] = False
                elif dt1 and st0 > dt1:
                    status['deployment_has_particles'] = False

                # Set the request start_date and end_date to the deployment window
                status['deployment_start_time'] = dt0.strftime('%Y-%m-%dT%H:%M:%S.%sZ')
                if dt1:
                    status['deployment_end_time'] = dt1.strftime('%Y-%m-%dT%H:%M:%S.%sZ')
                else:
                    status['deployment_end_time'] = dt1

                deployment_status['deployments'].append(status)

    if not deployment_status['deployments']:
        logging.warning('No valid instrument deployments found')
        return 0

    if args.csv:
        csv_writer = csv.writer(sys.stdout)
        csv_writer.writerow(deployment_status['deployments'][0].keys())
        for deployment in deployment_status['deployments']:
            csv_writer.writerow(deployment.values())
    else:
        sys.stdout.write('{:s}\n'.format(json.dumps(deployment_status, indent=4, sort_keys=True)))

    return 0


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('ref_des',
                            nargs='?',
                            type=str,
                            help='Partial or fully-qualified reference designator identifying one or more instruments')

    arg_parser.add_argument('-s', '--status',
                            dest='status',
                            type=str,
                            default='active',
                            choices=['active', 'inactive', 'all'],
                            help='Specify the status of the deployment')

    arg_parser.add_argument('-l', '--loglevel',
                            help='Verbosity level',
                            type=str,
                            choices=['debug', 'info', 'warning', 'error', 'critical'],
                            default='info')

    arg_parser.add_argument('--csv',
                            help='Print results as csv records',
                            action='store_true')

    arg_parser.add_argument('--stream',
                            type=str,
                            help='Restrict urls to the specified stream name, if it is produced by the instrument')

    arg_parser.add_argument('--telemetry',
                            type=str,
                            help='Restrict urls to the specified telemetry type')

    arg_parser.add_argument('-b', '--baseurl',
                            dest='base_url',
                            type=str,
                            help='UFrame base url beginning with http(s).  Taken from UFRAME_BASE_URL if not specified')

    arg_parser.add_argument('-t', '--timeout',
                            type=int,
                            default=30,
                            help='Request timeout, in seconds')

    arg_parser.add_argument('-d', '--direct',
                            dest='m2m',
                            action='store_false',
                            help='Send requests directly to UFrame, not via m2m (Not recommended)')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
