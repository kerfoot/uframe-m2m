#!/usr/bin/env python

import requests
import os
import sys
import argparse
import logging
import json
import csv
import urllib

# Disables SSL warnings
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

def main(args):
    '''Send the UFrame m2m API request_urls and print the server JSON responses
    to STDOUT'''
    
    # Set up the erddapfoo.lib.m2m.M2mClient logger
    log_level = getattr(logging, args.loglevel.upper())
    log_format = '%(module)s:%(levelname)s:%(message)s'
    logging.basicConfig(format=log_format, level=log_level)
    
    # Both args.username and args.password must be specified if either is specified
    auth = False
    if args.username and args.password:
        auth = True
    elif args.username and not args.password:
        logging.error('No API token specified')
        return 1
    elif args.password and not args.username:
        logging.error('No API username specified')
        return 1

    request_urls = args.request_urls
    if not request_urls and args.file:
        if not os.path.isfile(args.file):
            logging.error('Invalid request urls file ({:s})\n'.format(args.file))
            return 1
            
        try:
            with open(args.file, 'r') as fid:
                request_urls = fid.readlines()
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(args.file))
            return 1
            
    if not request_urls:
        logging.error('No request urls specified')
        return 1

    #sys.stdout.write('{:s}\n'.format(json.dumps(request_urls)))
    #return 13
    
    responses = []
    for url in request_urls:
        
        url = urllib.unquote(url.strip('/'))
        
        req = {u'url' : url,
            u'status_code' : None,
            u'response' : None}
        
        logging.debug('Sending GET request: {:s}'.format(url))
        
        try:
            if auth:
                logging.debug('GET with user-supplied credentials')
                r = requests.get(url, auth=(args.apiusername, args.apipassword), verify=False)
            else:
                # Assume .netrc configured
                logging.debug('GET with .netrc')
                r = requests.get(url, verify=False)
        except (requests.exceptions.MissingSchema, requests.exceptions.ConnectionError) as e:
            logging.error(e)
            continue
            
        req['status_code'] = r.status_code
        
        if r.status_code != 200:
            logging.error('Request {:s} failed ({:s})'.format(url, r.reason))
            req['response'] = r.text
            responses.append(req)
            continue
            
        try:
            req['response'] = r.json()
        except ValueError as e:
            logging.warning('{:s} ({:s})'.format(e, url))
            req['response'] = r.text
            
        responses.append(req)
        
    if not responses:
        return 1
        
    # Print responses as valid JSON
    sys.stdout.write('{:s}\n'.format(json.dumps(responses)))
            
    return 0

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('request_urls',
        nargs='*',
        type=str,
        help='A list of whitespace separated asynchronous UFrame request urls')
    arg_parser.add_argument('-u', '--apiusername',
        type=str,
        help='API username from the registered user profile settings.')
    arg_parser.add_argument('-p', '--apipassword',
        type=str,
        help='API token from the registered user profile settings')
    arg_parser.add_argument('-f', '--file',
        type=str,
        help='Filename containing the list of whitespace separated asynchronous UFrame request urls')
    arg_parser.add_argument('-o', '--outputfile',
        type=str,
        help='Write the UFrame JSON responses to the specified output file')
    arg_parser.add_argument('-t', '--timeout',
        type=int,
        default=120,
        help='Request timeout, in seconds <Default=120>')
    arg_parser.add_argument('-l', '--loglevel',
        help='Verbosity level <Default=warning>',
        type=str,
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='warning')

    parsed_args = arg_parser.parse_args()
    #print parsed_args
    #sys.exit(13)

    sys.exit(main(parsed_args))
