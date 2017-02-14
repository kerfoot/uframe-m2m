#!/usr/bin/env python

import os
import sys
import argparse
import logging
import json
import csv
from m2m.UFrameClient import UFrameClient
import urllib


def main(args):
    """Return the list of escaped request urls that conform to the UFrame API for the
        partial or fully-qualified reference_designator and all telemetry types.  
        The URLs request all stream L0, L1 and L2 dataset parameters over the entire 
        time-coverage.  The urls are printed to STDOUT."""

    # Set up the erddapfoo.lib.m2m.M2mClient logger
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

    urls = client.instrument_to_query(args.ref_des,
                                      user,
                                      stream=args.stream,
                                      telemetry=args.telemetry,
                                      time_delta_type=args.time_delta_type,
                                      time_delta_value=args.time_delta_value,
                                      begin_ts=args.start_date,
                                      end_ts=args.end_date,
                                      time_check=args.time_check,
                                      exec_dpa=args.no_dpa,
                                      application_type=args.format,
                                      provenance=args.no_provenance,
                                      limit=args.limit,
                                      annotations=args.no_annotations,
                                      email=args.email)

    if not args.raw:
        urls = [urllib.quote(u) for u in urls]

    if args.csv:
        for url in urls:
            sys.stdout.write('{:s}\n'.format(url))
    else:
        sys.stdout.write('{:s}\n'.format(json.dumps(urls)))

    return 0


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('ref_des',
                            type=str,
                            help='Partial or fully-qualified reference designator identifying one or more instruments')
    arg_parser.add_argument('-u', '--user',
                            type=str,
                            help='Specify the user for the request urls.  All fulfilled requests are written to this user directory. Taken from UFRAME_USER if not specified and set')
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
    arg_parser.add_argument('-s', '--start_date',
                            type=str,
                            help='An ISO-8601 formatted string specifying the start time/date for the data set')
    arg_parser.add_argument('-e', '--end_date',
                            type=str,
                            help='An ISO-8601 formatted string specifying the end time/data for the data set')
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
                            help='Specify the download format (<Default:netcdf> or json)')
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
