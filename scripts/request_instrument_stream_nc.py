#!/usr/bin/env python

import os
import sys
import argparse
import logging
import json
import csv
import re
import datetime
import requests
from m2m.UFrameClient import UFrameClient
import urllib


def main(args):
    """Send a single NetCDF request for the specified stream produced by the specified instrument (ref_des). The request
    response is written to the current working directory as valid JSON"""

    # Set up logging
    logger = logging.getLogger(__name__)
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(format=log_format, level=log_level)

    args.outputdir = args.outputdir or os.curdir

    uframe_base_url = args.base_url or os.getenv('UFRAME_BASE_URL')
    if not uframe_base_url:
        logging.error('No base_url set/found')
        return 1

    user = 'anonymous'

    client = UFrameClient(uframe_base_url, timeout=args.timeout, m2m=args.direct)

    # Make sure the reference designator is valid
    if args.ref_des not in client.instruments:
        logger.warning('Reference designator not found: {:s}'.format(args.ref_des))
        return 1

    # Fetch the streams produced by the instrument
    stream_metadata = client.fetch_instrument_streams(args.ref_des)
    if args.stream not in [s['stream'] for s in stream_metadata]:
        logger.warning('{:s} does not produce the specified stream: {:s}'.format(args.ref_des, args.stream))
        return 1

    urls = client.instrument_to_query(args.ref_des,
                                      user,
                                      stream=args.stream,
                                      time_delta_type=args.time_delta_type,
                                      time_delta_value=args.time_delta_value,
                                      begin_ts=args.start_date,
                                      end_ts=args.end_date,
                                      exec_dpa=args.no_dpa,
                                      provenance=args.no_provenance)

    if not urls:
        logging.warning('No valid NetCDF requests created for {:s}-{:s}'.format(args.ref_des, args.stream))

    # Dump the GET request only if args.printurl
    if args.printurl:
        if not args.raw:
            urls = [urllib.quote(u) for u in urls]
        if args.csv:
            for url in urls:
                sys.stdout.write('{:s}\n'.format(url))
        else:
            sys.stdout.write('{:s}\n'.format(json.dumps(urls)))
        return 0

    url = urls[0]
    req = {u'url': url,
           u'status_code': None,
           u'response': None,
           u'response_file': None}

    if not os.path.isdir(args.outputdir):
        logger.warning('Invalid response outputdir specified: {:s}'.format(args.outputdir))
        return 1
    args.outputdir = os.path.realpath(args.outputdir)

    # Create the request response file name
    response_path = '{:s}-{:s}-{:s}.request.json'.format(args.ref_des, args.stream,
                                                         datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S.%sZ'))
    req['response_file'] = os.path.join(args.outputdir, response_path)

    # Send the request
    logging.debug('Sending GET request: {:s}'.format(url))
    try:
        logging.debug('Sending request: {:s}'.format(url))
        r = requests.get(url, verify=False)
    except (requests.exceptions.MissingSchema, requests.exceptions.ConnectionError) as e:
        logging.error('{:}: {:s}'.format(e, url))
        return 1

    req['status_code'] = r.status_code

    if r.status_code != 200:
        logging.error('Request {:s} failed ({:})'.format(url, r.reason))
        req['response'] = r.text

    try:
        req['response'] = r.json()
    except ValueError as e:
        logging.warning('{:} ({:s})'.format(e, url))
        req['response'] = r.text

    try:
        with open(req['response_file'], 'w') as fid:
            json.dump(req, fid, indent=4, sort_keys=True)
            sys.stdout.write('{:s}\n'.format(req['response_file']))
    except IOError as e:
        logging.error('Error writing response file ({:}): {:s}'.format(e, req['response_file']))
        return 1

    return 0


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('ref_des',
                            type=str,
                            help='Fully-qualified reference designator identifying an instrument')

    arg_parser.add_argument('stream',
                            type=str,
                            help='Stream name')

    arg_parser.add_argument('--outputdir',
                            type=str,
                            help='Write the UFrame JSON response to outputdir')

    arg_parser.add_argument('-p', '--printurl',
                            action='store_true',
                            help='Print request url to STDOUT, but do not send the request')

    arg_parser.add_argument('-r', '--raw',
                            action='store_true',
                            help='Request urls are escaped by default, preventing shell interference when sending them from the command line.  This option disables this behavior')

    arg_parser.add_argument('--csv',
                            help='Print results as csv records',
                            action='store_true')

    arg_parser.add_argument('-s', '--start_date',
                            type=str,
                            help='An ISO-8601 formatted string specifying the start time/date for the data set')

    arg_parser.add_argument('-e', '--end_date',
                            type=str,
                            help='An ISO-8601 formatted string specifying the end time/data for the data set')

    arg_parser.add_argument('--time_delta_type',
                            type=str,
                            help='Time delta type for calculating the subset start time',
                            choices=['minutes', 'hours', 'days', 'weeks'],
                            default='days')

    arg_parser.add_argument('--time_delta_value',
                            type=int,
                            default=1,
                            help='Positive integer value to subtract from the end time to get the request start time.')

    arg_parser.add_argument('--no_dpa',
                            action='store_false',
                            default=True,
                            help='Execute all data product algorithms to return L1/L2 parameters')

    arg_parser.add_argument('--no_provenance',
                            action='store_false',
                            default=True,
                            help='Include provenance information in the data sets')

    arg_parser.add_argument('-b', '--baseurl',
                            dest='base_url',
                            type=str,
                            help='UFrame base url beginning with https.  Taken from UFRAME_BASE_URL if not specified')

    arg_parser.add_argument('-t', '--timeout',
                            type=int,
                            default=30,
                            help='Request timeout, in seconds')

    arg_parser.add_argument('--email',
                            dest='email',
                            type=str,
                            help='Add an email address for emailing UFrame responses to the request once sent')

    arg_parser.add_argument('-d', '--direct',
                            action='store_false',
                            help='Send requests directly to UFrame, not via m2m (Not recommended)')

    arg_parser.add_argument('-l', '--loglevel',
                            help='Verbosity level <Default=warning>',
                            type=str,
                            choices=['debug', 'info', 'warning', 'error', 'critical'],
                            default='warning')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
