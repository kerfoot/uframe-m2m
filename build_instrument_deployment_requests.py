#!/usr/bin/env python

import os
import sys
import argparse
import logging
import json
import csv
import datetime
from m2m.UFrameClient import UFrameClient
import urllib


def main(args):
    """Return the list of escaped request urls that conform to the UFrame API for the
        partial or fully-qualified reference_designator and all telemetry types.  
        The URLs request all stream L0, L1 and L2 dataset parameters over the entire 
        time-coverage.  The urls are printed to STDOUT."""

    # Set up the lib.m2m.M2mClient logger
    logger = logging.getLogger(__name__)
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

    user = args.user
    if not user:
        if not os.getenv('UFRAME_USER'):
            logger.error('No user specified and UFRAME_USER not set')
            return 1
        user = os.getenv('UFRAME_USER')

    client = UFrameClient(uframe_base_url, timeout=args.timeout)

    # Find all fully qualified reference designators
    all_deployments = client.fetch_instrument_deployments(args.ref_des)
    if not all_deployments:
        logger.debug('No instruments matching ref_des {:s}'.format(args.ref_des))
        return 0

    if args.status == 'active':
        all_deployments = [d for d in all_deployments if not d['eventStopTime']]
    elif args.status == 'inactive':
        all_deployments = [d for d in all_deployments if d['eventStopTime']]

    # Set to args.stream to create urls for this stream only if specified.  Otherwise urls for all streams produced
    # by the instrument will be created.
    stream_name = args.stream

    request_urls = []
    for d in all_deployments:

        # Handle the inconsistent nature of the deployment asset management schema
        if type(d['referenceDesignator']) == dict:
            d['ref_des'] = '-'.join([d['referenceDesignator']['subsite'],
                                     d['referenceDesignator']['node'],
                                     d['referenceDesignator']['sensor']])
        else:
            d['ref_des'] = d['referenceDesignator']

        logger.debug('{:s} Deployment {:0.0f}'.format(d['ref_des'], d['deploymentNumber']))

        # Create the event start timestamp
        try:
            d['eventStartTs'] = datetime.datetime.utcfromtimestamp(d['eventStartTime'] / 1000).strftime(
                '%Y-%m-%dT%H:%M:%SZ')
        except ValueError as e:
            logging.warning(e)
            continue

        # Create the event start timestamp
        d['eventStopTs'] = None
        if d['eventStopTime']:
            d['active'] = False
            try:
                d['eventStopTs'] = datetime.datetime.utcfromtimestamp(d['eventStopTime'] / 1000).strftime(
                    '%Y-%m-%dT%H:%M:%SZ')
            except ValueError as e:
                logging.warning(e)
                continue

        urls = client.instrument_to_query(d['ref_des'],
                                          args.user,
                                          stream=stream_name,
                                          telemetry=args.telemetry,
                                          time_delta_type=None,
                                          time_delta_value=None,
                                          begin_ts=d['eventStartTs'],
                                          end_ts=d['eventStopTs'],
                                          time_check=args.time_check,
                                          exec_dpa=args.no_dpa,
                                          application_type=args.format,
                                          provenance=args.no_provenance,
                                          limit=args.limit,
                                          annotations=args.no_annotations,
                                          email=args.email)

        if not urls:
            logger.debug('No deployment request urls created ({:s})'.format(d['ref_des']))
            continue

        request_urls = request_urls + urls

    if not args.raw:
        request_urls = [urllib.quote(u) for u in request_urls]

    if args.csv:
        for url in request_urls:
            sys.stdout.write('{:s}\n'.format(url))
    else:
        sys.stdout.write('{:s}\n'.format(json.dumps(request_urls)))

    return 0


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('ref_des',
                            nargs='+',
                            type=str,
                            help='Partial or fully-qualified reference designator identifying one or more instruments')
    arg_parser.add_argument('-u', '--user',
                            type=str,
                            help='Specify the user for the request urls.  All fulfilled requests are written to this user directory. Taken from UFRAME_USER if not specified and set')
    arg_parser.add_argument('-s', '--status',
                            dest='status',
                            type=str,
                            default='all',
                            choices=['active', 'inactive', 'all'],
                            help='Specify the status of the deployment <Default=all>')
    arg_parser.add_argument('-r', '--raw',
                            action='store_true',
                            help='Request urls are escaped by default, preventing shell interference when sending them from the command line.  This option disables this behavior')
    arg_parser.add_argument('-l', '--loglevel',
                            help='Verbosity level <Default=warning>',
                            type=str,
                            choices=['debug', 'info', 'warning', 'error', 'critical'],
                            default='warning')
    arg_parser.add_argument('--csv',
                            help='Print results as csv records',
                            action='store_true')
    arg_parser.add_argument('--stream',
                            type=str,
                            help='Restricts urls to the specified stream name, if it is produced by the instrument')
    arg_parser.add_argument('--telemetry',
                            type=str,
                            help='Restricts urls to the specified telemetry type')
    arg_parser.add_argument('--time_delta_type',
                            type=str,
                            help='Type for calculating the subset start time, i.e.: years, months, weeks, days.  Must be a type kwarg accepted by dateutil.relativedelta')
    arg_parser.add_argument('--time_delta_value',
                            type=int,
                            help='Positive integer value to subtract from the end time to get the start time for subsetting.')
    arg_parser.add_argument('--no_time_check',
                            dest='time_check',
                            default=True,
                            action='store_false',
                            help='Do not replace invalid request start and end times with stream metadata values if they fall out of the stream time coverage')
    arg_parser.add_argument('--no_dpa',
                            action='store_false',
                            default=True,
                            help='Execute all data product algorithms to return L1/L2 parameters <Default:False>')
    arg_parser.add_argument('--no_provenance',
                            action='store_false',
                            default=True,
                            help='Include provenance information in the data sets <Default:False>')
    arg_parser.add_argument('-f', '--format',
                            dest='format',
                            type=str,
                            default='netcdf',
                            choices=['netcdf', 'json'],
                            help='Specify the download format <Default:netcdf>')
    arg_parser.add_argument('--no_annotations',
                            action='store_false',
                            default=False,
                            help='Include all annotations in the data sets <Default>:False')
    arg_parser.add_argument('--limit',
                            type=int,
                            default=-1,
                            help='Integer ranging from -1 to 10000.  <Default:-1> results in a non-decimated dataset')
    arg_parser.add_argument('-b', '--baseurl',
                            dest='base_url',
                            type=str,
                            help='UFrame m2m base url beginning with https.  Taken from UFRAME_BASE_URL if not specified')
    arg_parser.add_argument('-t', '--timeout',
                            type=int,
                            default=120,
                            help='Request timeout, in seconds <Default:120>')
    arg_parser.add_argument('--email',
                            dest='email',
                            type=str,
                            help='Add an email address for emailing UFrame responses to the request once sent')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
