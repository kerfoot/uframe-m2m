import logging
import requests
import re
from dateutil import parser
from dateutil.relativedelta import relativedelta as tdelta
from pytz import timezone

# Disables SSL warnings
import requests.packages.urllib3

requests.packages.urllib3.disable_warnings()

HTTP_STATUS_OK = 200

_valid_relativedeltatypes = ('years',
                             'months',
                             'weeks',
                             'days',
                             'hours',
                             'minutes',
                             'seconds')


class UFrameClient(object):
    """Lightweight OOI UFrame client for making GET requests to the UFrame API via
    the machine to machine (m2m) API or directly to UFrame.
    
    Parameters:
        base_url: UFrame API base url which must begin with https://
        
    kwargs:
        m2m: If true <Default>, specifies that all requests should be created and sent throught the m2m API
        timeout: request timeout, in seconds
        api_username: API username from the UI user settings
        api_token: API password from the UI user settings
    """

    def __init__(self, base_url, m2m=True, timeout=120, api_username=None, api_token=None):

        self._base_url = None
        self._m2m_base_url = None
        self._timeout = timeout
        self._api_username = api_username
        self._api_token = api_token
        self._session = requests.Session()
        self._is_m2m = m2m
        self._instruments = []
        self._subsites = []

        self._logger = logging.getLogger(__name__)

        # properties for last m2m request
        self._request_url = None
        self._response = None
        self._status_code = None
        self._response_headers = None

        # Set the base url
        self._logger.debug('Creating M2mClient instance ({:s})'.format(base_url))
        self.base_url = base_url

    @property
    def base_url(self):
        return self._base_url

    @base_url.setter
    def base_url(self, url):

        self._logger.debug('Setting UFrame base url: {:s}'.format(url))

        if not url:
            self._logger.warning('No UFrame base_url specified')
            return
        if not url.startswith('http'):
            self._logger.warning('base_url must start with http')
            return

        self._base_url = url.strip('/')
        self._m2m_base_url = '{:s}/api/m2m'.format(self._base_url)

        self._logger.debug('UFrame base_url: {:s}'.format(self.base_url))
        self._logger.debug('UFrame m2m base_url: {:s}'.format(self.m2m_base_url))

        # Try to get the sensor invetory subsite list to see if we're able to connect
        self.fetch_sensor_subsites()
        if self._status_code != HTTP_STATUS_OK:
            self._logger.critical('Unable to connect to UFrame instance')
            return

        # Create the instrument list
        self._create_instrument_list()

    @property
    def is_m2m(self):
        return self._is_m2m

    @is_m2m.setter
    def is_m2m(self, status):
        if type(status) != bool:
            self._logger.error('status must be True or False')
            return

        self._is_m2m = status

    @property
    def m2m_base_url(self):
        return self._m2m_base_url

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, seconds):
        if type(seconds) != int:
            self._logger.warning('timeout must be an integer')
            return

        self._timeout = seconds

    @property
    def last_request_url(self):
        return self._request_url

    @property
    def last_response(self):
        return self._response

    @property
    def last_status_code(self):
        return self._status_code

    @property
    def instruments(self):
        return self._instruments

    def fetch_sensor_subsites(self):
        """Fetch all registered subsites from the /sensor/inv API endpoint"""

        self._logger.debug('Fetching sensor subsites')

        port = 12576
        end_point = '/sensor/inv'

        request_url = self.build_request(port,
                                         end_point)

        return self.send_request(request_url)

    def fetch_deployment_subsites(self):
        """Fetch all registered subsites from the /events/deployment/inv API
        endpoint"""

        self._logger.debug('Fetching deployment subsites')

        port = 12587
        end_point = '/events/deployment/inv'

        request_url = self.build_request(port,
                                         end_point)

        return self.send_request(request_url)

    def fetch_instrument_streams(self, ref_des):
        """Fetch all streams produced by the fully-qualified reference designator"""

        self._logger.debug('{:s} - Fetching instrument streams'.format(ref_des))

        r_tokens = ref_des.split('-')

        port = 12576
        end_point = '/sensor/inv/{:s}/{:s}/{:s}-{:s}/metadata/times'.format(r_tokens[0],
                                                                            r_tokens[1],
                                                                            r_tokens[2],
                                                                            r_tokens[3])

        request_url = self.build_request(port,
                                         end_point)

        return self.send_request(request_url)

    def fetch_instrument_parameters(self, ref_des):
        """Fetch all parameters in the streams produced by the fully-qualified
        reference designator"""

        self._logger.debug('{:s} - Fetching instrument parameters'.format(ref_des))

        r_tokens = ref_des.split('-')

        port = 12576
        end_point = '/sensor/inv/{:s}/{:s}/{:s}-{:s}/metadata/parameters'.format(r_tokens[0],
                                                                                 r_tokens[1],
                                                                                 r_tokens[2],
                                                                                 r_tokens[3])

        request_url = self.build_request(port,
                                         end_point)

        return self.send_request(request_url)

    def fetch_instrument_metadata(self, ref_des):
        """Fetch all streams and all parameters produced by the fully-qualified
        reference designator"""

        self._logger.debug('{:s} - Fetching instrument metadata'.format(ref_des))

        r_tokens = ref_des.split('-')

        port = 12576
        end_point = '/sensor/inv/{:s}/{:s}/{:s}-{:s}/metadata'.format(r_tokens[0],
                                                                      r_tokens[1],
                                                                      r_tokens[2],
                                                                      r_tokens[3])

        request_url = self.build_request(port,
                                         end_point)

        return self.send_request(request_url)

    def fetch_instrument_deployments(self, ref_des):
        """Fetch all deployment events for the fully or partially qualified reference designator"""

        self._logger.debug('{:s} - Fetching instrument deployments'.format(ref_des))

        port = 12587
        end_point = '/events/deployment/query?refdes={:s}'.format(ref_des)

        request_url = self.build_request(port,
                                         end_point)

        return self.send_request(request_url)

    def search_instruments(self, ref_des):
        """Search all instruments for the fully-qualified reference designators
        matching the fully or partially-qualified ref_des string"""

        return [i for i in self._instruments if i.find(ref_des) > -1]

    def build_and_send_request(self, port, end_point):
        """Build and send the request url for the specified port and end_point"""

        url = self.build_request(port, end_point)

        return self.send_request(url)

    def build_request(self, port, end_point):
        """Build the request url for the specified port and end_point"""

        if self._is_m2m:
            url = '{:s}/{:0.0f}/{:s}'.format(self._m2m_base_url, port, end_point.strip('/'))
        else:
            url = '{:s}:{:0.0f}/{:s}'.format(self._base_url, port, end_point.strip('/'))

        return url

    def send_request(self, url):
        """Send the request url through either the m2m API or directly to UFrame.
        The method used is determined by the is_m2m property.  If set to True, the
        request is sent through the m2m API.  If set to False, the request is sent
        directly to UFrame"""

        self._request_url = url
        self._response = None
        self._status_code = None
        self._response_headers = None

        if self.is_m2m and not url.startswith(self.m2m_base_url):
            self._logger.error('URL does not point to the m2m base url ({:s})'.format(self.m2m_base_url))
            return
        elif not url.startswith(self.base_url):
            self._logger.error('URL does not point to the base url ({:s})'.format(self.base_url))
            return

        try:
            self._logger.debug('Sending GET request: {:s}\n'.format(url))
            if self._api_username and self._api_token:
                r = self._session.get(url,
                                      auth=(self._api_username,
                                            self._api_token),
                                      timeout=self._timeout,
                                      verify=False)
            else:
                r = self._session.get(url, timeout=self._timeout, verify=False)
        except (requests.exceptions.MissingSchema, requests.exceptions.ConnectionError) as e:
            self._logger.error('{:s} - {:s}'.format(e, url))
            return

        self._status_code = r.status_code
        if self._status_code != HTTP_STATUS_OK:
            self._logger.error('Request failed ({:s})'.format(r.reason))
            return

        self._response_headers = r.headers

        try:
            self._response = r.json()
        except ValueError as e:
            self._logger.warning('{:s} ({:s})'.format(e, url))
            self._response = r.text

        return self._response

    def _create_instrument_list(self):

        self._instruments = []

        url = self.build_request(12576,
                                 '/sensor/allstreams')

        self._logger.debug('Creating instruments list')
        self.send_request(url)

        instrument_regex = re.compile('>(\w+/\w+/\w+\-\w+)<')
        matches = instrument_regex.findall(self._response)
        if not matches:
            return

        instruments = ['-'.join(m.split('/')) for m in matches]
        instruments.sort()

        self._instruments = instruments

    def instrument_to_query(self, ref_des, user, stream=None, telemetry=None, time_delta_type=None,
                            time_delta_value=None, begin_ts=None, end_ts=None, time_check=True, exec_dpa=True,
                            application_type='netcdf', provenance=True, limit=-1, annotations=False, email=None):
        """Return the list of request urls that conform to the UFrame API for the specified
        fully or paritally-qualified reference_designator.  Request urls are formatted
        for either the UFrame m2m API (default) or direct UFrame access, depending
        on is_m2m property of the UFrameClient instance.
        
        Arguments:
            ref_des: partial or fully-qualified reference designator
            stream: restrict urls to the specified stream
            user: user name for the query
            
        Optional kwargs:
            telemetry: telemetry type (Default is all telemetry types
            time_delta_type: Type for calculating the subset start time, i.e.: years, months, weeks, days.  Must be a
                type kwarg accepted by dateutil.relativedelta'
            time_delta_value: Positive integer value to subtract from the end time to get the start time for subsetting.
            begin_ts: ISO-8601 formatted datestring specifying the dataset start time
            end_ts: ISO-8601 formatted datestring specifying the dataset end time
            time_check: set to true (default) to ensure the request times fall within the stream data availability
            exec_dpa: boolean value specifying whether to execute all data product algorithms to return L1/L2 parameters
                (Default is True)
            application_type: 'netcdf' or 'json' (Default is 'netcdf')
            provenance: boolean value specifying whether provenance information should be included in the data set
                (Default is True)
            limit: integer value ranging from -1 to 10000.  A value of -1 (default) results in a non-decimated dataset
            annotations: boolean value (True or False) specifying whether to include all dataset annotations
        """

        urls = []

        instruments = self.search_instruments(ref_des)
        if not instruments:
            return urls

        if time_delta_type and time_delta_value:
            if time_delta_type not in _valid_relativedeltatypes:
                self._logger.error('Invalid dateutil.relativedelta type: {:s}'.format(time_delta_type))
                return urls

        begin_dt = None
        end_dt = None
        if begin_ts:
            try:
                begin_dt = parser.parse(begin_ts)
            except ValueError as e:
                self._logger.error('Invalid begin_dt: {:s} ({:s})'.format(begin_ts, e.message))
                return urls

        if end_ts:
            try:
                end_dt = parser.parse(end_ts)
            except ValueError as e:
                self._logger.error('Invalid end_dt: {:s} ({:s})'.format(end_ts, e.message))
                return urls

        for instrument in instruments:

            # Get the streams produced by this instrument
            instrument_streams = self.fetch_instrument_streams(instrument)
            if not instrument_streams:
                self._logger.info('No streams found for {:s}'.format(instrument))
                continue

            if stream:
                stream_names = [s['stream'] for s in instrument_streams]
                if stream not in stream_names:
                    self._logger.warning('Invalid stream: {:s}-{:s}'.format(instrument, stream))
                    continue

                i = stream_names.index(stream)
                instrument_streams = [instrument_streams[i]]

            if not instrument_streams:
                self._logger.info('{:s}: No streams found'.format(instrument))
                continue

            # Break the reference designator up
            r_tokens = instrument.split('-')

            for instrument_stream in instrument_streams:

                if telemetry and instrument_stream['method'].find(telemetry) == -1:
                    continue

                # Figure out what we're doing for time
                try:
                    stream_dt0 = parser.parse(instrument_stream['beginTime'])
                except ValueError:
                    self._logger.error(
                        '{:s}-{:s}: Invalid beginTime ({:s})'.format(
                            instrument, instrument_stream['stream'], instrument_stream['beginTime']))
                    continue

                try:
                    stream_dt1 = parser.parse(instrument_stream['endTime'])
                except ValueError:
                    self._logger.error(
                        '{:s}-{:s}: Invalid endTime ({:s})'.format(
                            'instrument', instrument_stream['stream'], instrument_stream['endTime']))
                    continue

                if time_delta_type and time_delta_value:
                    dt1 = stream_dt1
                    dt0 = dt1 - tdelta(**dict({time_delta_type: time_delta_value}))
                else:
                    if begin_dt:
                        dt0 = begin_dt
                    else:
                        dt0 = stream_dt0

                    if end_dt:
                        dt1 = end_dt
                    else:
                        dt1 = stream_dt1

                # Format the endDT and beginDT values for the query
                try:
                    ts1 = dt1.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError as e:
                    self._logger.error('{:s}-{:s}: {:s}'.format(instrument, instrument_stream['stream'], e.message))
                    continue

                try:
                    ts0 = dt0.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError as e:
                    self._logger.error('{:s}-{:s}: {:s}'.format(instrument, instrument_stream['stream'], e.message))
                    continue

                # Make sure the specified or calculated start and end time are within
                # the stream metadata times if time_check=True
                if time_check:
                    if dt1 > stream_dt1:
                        self._logger.warning(
                            'time_check ({:s}-{:s}): End time exceeds stream endTime ({:s} > {:s})'.format(
                                ref_des, instrument_stream['stream'], ts1, instrument_stream['endTime']))
                        self._logger.warning(
                            'time_check ({:s}-{:s}): Setting request end time to stream endTime'.format(
                                ref_des, instrument_stream['stream']))
                        ts1 = instrument_stream['endTime']

                    if dt0 < stream_dt0:
                        self._logger.warning(
                            'time_check ({:s}-{:s}): Start time is earlier than stream beginTime ({:s} < {:s})'.format(
                                ref_des, instrument_stream['stream'], ts0, instrument_stream['beginTime']))
                        self._logger.warning(
                            'time_check ({:s}-{:s}): Setting request begin time to stream beginTime'.format(
                                ref_des, instrument_stream['stream']))
                        ts0 = instrument_stream['beginTime']

                    # Check that ts0 < ts1
                    dt0 = parser.parse(ts0)
                    dt1 = parser.parse(ts1)
                    if dt0 >= dt1:
                        self._logger.error(
                            '{:s}: Invalid time range specified ({:s} >= {:s})'.format(
                                instrument_stream['stream'], ts0, ts1))
                        continue

                # Create the url
                end_point = 'sensor/inv/{:s}/{:s}/{:s}-{:s}/{:s}/{:s}?beginDT={:s}&endDT={:s}&format=application/{:s}&limit={:d}&execDPA={:s}&include_provenance={:s}&user={:s}'.format(
                    r_tokens[0],
                    r_tokens[1],
                    r_tokens[2],
                    r_tokens[3],
                    instrument_stream['method'],
                    instrument_stream['stream'],
                    ts0,
                    ts1,
                    application_type,
                    limit,
                    str(exec_dpa).lower(),
                    str(provenance).lower(),
                    user)

                if email:
                    end_point = '{:s}&email={:s}'.format(end_point, email)

                urls.append(self.build_request(12576, end_point))

        return urls

    def __repr__(self):
        return '<UFrameClient(url={:s}, m2m={:s})>'.format(self.base_url, str(self._is_m2m))
