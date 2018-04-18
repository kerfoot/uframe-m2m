#!/usr/bin/env python

import os
import sys
import argparse
import logging
import json
import csv
import copy
from m2m.UFrameClient import UFrameClient


def main(args):
    """Search for instruments that produce a stream containing the parameter names that contain one or more
    parameter_search_terms"""

    # Translate the logging level string to numeric value
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(asctime)s:%(module)s:%(levelname)s:%(message)s [line %(lineno)d]'
    logging.basicConfig(level=log_level, format=log_format)

    uframe_base_url = args.base_url or os.getenv('UFRAME_BASE_URL')
    m2m = args.direct
    timeout = args.timeout
    ooi_array = args.ooi_array
    ref_des_term = args.ref_des_term
    search_terms = args.parameter_search_terms
    telemetry = args.telemetry
    client = UFrameClient(uframe_base_url, timeout=timeout, m2m=m2m)

    # UFrameClient instance
    client = UFrameClient(uframe_base_url, timeout=args.timeout, m2m=args.direct)
    if not client.base_url:
        return 1

    # Get the table of contents
    toc = client.toc
    # Get the particle_key (parameter name)
    parameters = []
    for term in search_terms:
        found_pd_ids = [{'pdId': p['pdId'], 'particle_key': p['particle_key']} for p in toc['parameter_definitions'] if
                        p['particle_key'].find(term) > -1]
        if not found_pd_ids:
            continue

        parameters = parameters + found_pd_ids

    # Unique list of pd ids
    pd_ids = list(set([p['pdId'] for p in parameters]))

    target_streams = []
    for pd_id in pd_ids:
        pd_id_streams = [k for k in toc['parameters_by_stream'] if pd_id in toc['parameters_by_stream'][k]]
        if not pd_id_streams:
            continue
        target_streams = target_streams + pd_id_streams

    # Unique list of streams that have at least one pd id in them
    target_streams = list(set(target_streams))
    target_streams.sort()

    # Loop through toc['instruments'] to see if it produces one or more stream
    parameter_instruments = []
    found_instruments = []
    for instrument in toc['instruments']:

        if ooi_array and not instrument['reference_designator'].startswith(ooi_array.upper()):
            continue

        if ref_des_term and instrument['reference_designator'].find(ref_des_term.upper()) == -1:
            continue

        instrument_streams = [s['stream'] for s in instrument['streams']]
        for instrument_stream in instrument_streams:
            if instrument_stream not in target_streams:
                continue

            if instrument['reference_designator'] not in found_instruments:
                instrument_metadata = copy.copy(instrument)
                instrument_metadata['streams'] = []
                parameter_instruments.append(instrument_metadata)
                found_instruments.append(instrument_metadata['reference_designator'])

            instrument_i = found_instruments.index(instrument_metadata['reference_designator'])

            if instrument_stream in [s['stream'] for s in parameter_instruments[instrument_i]['streams']]:
                continue

            stream_i = instrument_streams.index(instrument_stream)
            if telemetry and instrument['streams'][stream_i]['method'] != telemetry:
                continue

            parameter_instruments[instrument_i]['streams'].append(instrument['streams'][stream_i])

    if args.csv:
        if not parameter_instruments:
            return 0

        csv_writer = csv.writer(sys.stdout)
        cols = ['stream',
                'method',
                'beginTime',
                'endTime',
                'count']
        csv_writer.writerow(['reference_designator'] + cols)
        for instrument in parameter_instruments:
            for stream in instrument['streams']:
                stream_cols = [instrument['reference_designator']] + [stream[c] for c in cols]
                csv_writer.writerow(stream_cols)

    else:
        sys.stdout.write('{:s}\n'.format(json.dumps(parameter_instruments, sort_keys=True, indent=4)))

    return 0


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('parameter_search_terms',
                            nargs='+',
                            help='One or more parameter search terms')

    arg_parser.add_argument('-a', '--array',
                            dest='ooi_array',
                            help='First 2 characters of the OOI array',
                            choices=['ce', 'cp', 'ga', 'gi', 'gp', 'gs', 'rs'])

    arg_parser.add_argument('-r', '--ref_des',
                            dest='ref_des_term',
                            help='Full or partial reference designator to further refine the search')

    arg_parser.add_argument('--method',
                            dest='telemetry',
                            help='Telemetry type',
                            choices=['telemetered', 'recovered', 'recovered_host', 'streamed'])

    arg_parser.add_argument('-b', '--baseurl',
                            dest='base_url',
                            type=str,
                            help='UFrame base url beginning with https.  Taken from UFRAME_BASE_URL if not specified')

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

    # print(parsed_args)
    # sys.exit(13)

    sys.exit(main(parsed_args))
