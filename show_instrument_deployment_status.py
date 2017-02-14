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
from collections import OrderedDict


def main(args):
    """Show the deployment status and stream particle overlap for all instruments"""

    HTTP_STATUS_OK = 200
<<<<<<< HEAD
    
=======

>>>>>>> 8a616a33d3e2643cf2518fbfd408537545cdd883
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

    client = UFrameClient(uframe_base_url, timeout=args.timeout, m2m=args.m2m)

    ref_des = args.ref_des
    if not ref_des:
        instruments = client.instruments
    else:
        instruments = client.search_instruments(args.ref_des)
        
    deployment_status = []
    for instrument in instruments:
        
        # Find all fully qualified reference designators
        all_deployments = client.fetch_instrument_deployments(instrument)
        if client.last_status_code != HTTP_STATUS_OK:
            continue
<<<<<<< HEAD
            
=======
        
>>>>>>> 8a616a33d3e2643cf2518fbfd408537545cdd883
        if not all_deployments:
            logger.debug('No deployments found for instrument {:s}'.format(instrument))
            continue
    
        if args.status == 'active':
            all_deployments = [d for d in all_deployments if not d['eventStopTime']]
        elif args.status == 'inactive':
            all_deployments = [d for d in all_deployments if d['eventStopTime']]
            
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
                dt1 = None
    
            # Loop through each stream
            for stream in streams:
                
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
                
                if not dt1:
                    status['active'] = True
                    
                # Parse the stream beginTime
                try:
                    st0 = parser.parse(stream['beginTime'])
                except ValueError as e:
                    logger.error('{:s} beginTime parse error - {:s} ({:s})'.format(stream['stream'], e, stream['endTime']))
                    continue
                    
                # Parse the stream beginTime
                try:
                    st1 = parser.parse(stream['endTime'])
                except ValueError as e:
                    logger.error('{:s} endTime parse error - {:s} ({:s})'.format(stream['stream'], e, stream['endTime']))
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
        
                deployment_status.append(status)

    if not deployment_status:
        logging.warning('No valid instrument deployments found')
        return 0
        
    if args.csv:
        csv_writer = csv.writer(sys.stdout)
        csv_writer.writerow(deployment_status[0].keys())
        for deployment in deployment_status:
            csv_writer.writerow(deployment.values())
    else:
        sys.stdout.write('{:s}\n'.format(json.dumps(deployment_status)))

    return 0


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('ref_des',
                            nargs='?',
                            type=str,
                            help='Partial or fully-qualified reference designator identifying one or more instruments')
    arg_parser.add_argument('-s', '--status',
                            dest='status',
                            type=str,
                            default='all',
                            choices=['active', 'inactive', 'all'],
                            help='Specify the status of the deployment <Default=all>')
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
    arg_parser.add_argument('-b', '--baseurl',
                            dest='base_url',
                            type=str,
                            help='UFrame m2m base url beginning with https.  Taken from UFRAME_BASE_URL if not specified')
    arg_parser.add_argument('-t', '--timeout',
                            type=int,
                            default=120,
                            help='Request timeout, in seconds <Default:120>')
    arg_parser.add_argument('-d', '--direct',
        dest='m2m',
        action='store_false',
        help='Send requests directly to UFrame, not via m2m (Not recommended)')
    
    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
