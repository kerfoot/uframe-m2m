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
import pytz
from dateutil import parser


def main(args):
    """Return all asynchronous NetCDF data set request urls for all streams produced by the specified reference designator
    for all deployments of the instrument(s)"""

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

    client = UFrameClient(uframe_base_url, timeout=args.timeout, m2m=args.direct)

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

        logger.info('{:s} (Deployment {:0.0f})'.format(d['ref_des'], d['deploymentNumber']))

        streams = client.fetch_instrument_streams(d['ref_des'])
        if not streams:
            logger.warning('No streams found for deployed instrument')
            continue
            
        # Deployment event must have a parseable start time
        if not d['eventStartTime']:
            logger.warning('Deployment event has no eventStartTime')
            continue
            
        # Parse eventStartTime
        try:
            dt0 = datetime.datetime.utcfromtimestamp(d['eventStartTime']/1000).replace(tzinfo=pytz.UTC)
        except ValueError as e:
            logging.error(e)
            continue
        # Parse eventStopTime if there is one
        if d['eventStopTime']:
            try:
                dt1 = datetime.datetime.utcfromtimestamp(d['eventStopTime']/1000).replace(tzinfo=pytz.UTC)
            except ValueError as e:
                logging.error(e)
                continue
        else:
            dt1 = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)

        # Loop through each stream
        for stream in streams:
            
            # Skip this stream if the user specified a target stream and this is
            # not it
            if args.stream and args.stream.find(stream['stream']) == -1:
                logger.debug('Skipping unwanted stream ({:s})'.format(stream['stream']))
                continue
                
            # Parse the stream beginTime
            try:
                st0 = parser.parse(stream['beginTime'])
            except ValueError as e:
                logger.error('Stream ({:s}) beginTime parse error - {:s}'.format(stream['stream'], e))
                continue
                
            # Parse the stream beginTime
            try:
                st1 = parser.parse(stream['endTime'])
            except ValueError as e:
                logger.error('Stream ({:s}) endTime parse error - {:s}'.format(stream['stream'], e))
                continue
                
            # Check stream endTime to make sure it's not before the deployment began
            if st1 < dt0:
                logger.warning('Stream ({:s}) ends before deployment event begins'.format(stream['stream']))
                continue
                
            # Set the request start_date and end_date to the deployment window
            start_date = dt0.strftime('%Y-%m-%dT%H:%M:%S.%sZ')
            end_date = dt1.strftime('%Y-%m-%dT%H:%M:%S.%sZ')
            
            urls = client.instrument_to_query(d['ref_des'],
                user,
                stream=stream['stream'],
                telemetry=args.telemetry,
                time_delta_type=None,
                time_delta_value=None,
                begin_ts=start_date,
                end_ts=end_date,
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
    arg_parser.add_argument('-d', '--direct',
        action='store_false',
        help='Send requests directly to UFrame, not via m2m (Not recommended)')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
