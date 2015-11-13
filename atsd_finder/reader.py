import ConfigParser
import fnmatch
import re
import os
import time
import datetime
import calendar
import pytz

from . import utils
from graphite.intervals import Interval, IntervalSet

try:
    from graphite.logger import log
    # noinspection PyUnresolvedReferences
    from django.conf import settings
    from graphite.readers import FetchInProgress
except:  # debug env
    from graphite import settings
    import default_logger as log
    from sample import FetchInProgress
    log.info('[AtsdReader] reader running in debug environment')


def strf_timestamp(sec):
    return datetime.datetime.fromtimestamp(sec).strftime('%d %m %Y %H:%M:%S')


def _time_minus_months(ts, months):
    """substract given number of months from timestamp

    :param ts: `Number` timestamp in seconds
    :param months: `int` months to substract
    :return: `Number` timestamp in seconds
    """

    months = int(months)
    tz_utc = pytz.timezone('UTC')
    tz_local = pytz.timezone(settings.TIME_ZONE)

    dt_naive = datetime.datetime.utcfromtimestamp(ts)  # no tz
    dt_utc = dt_naive.replace(tzinfo=tz_utc)
    dt_local = dt_utc.astimezone(tz_local)

    month = dt_local.month - months - 1  # month + 12*year_delta - 1
    year = dt_local.year + month // 12
    month = month % 12 + 1
    day = min(dt_local.day, calendar.monthrange(year, month)[1])

    resdt_local = datetime.datetime(year,
                                    month,
                                    day,
                                    dt_local.hour,
                                    dt_local.minute,
                                    dt_local.second).replace(tzinfo=tz_local)
    resdt_utc = resdt_local.astimezone(tz_utc)

    log.info('[AtsdReader] {0} - {1}mon - {2}'
             .format(dt_local, months, resdt_local))

    return calendar.timegm(resdt_utc.timetuple())


def _time_minus_days(ts, days):
    """substract given number of days from timestamp

    :param ts: `Number` timestamp in seconds
    :param days: `int` days to substract
    :return: `Number` timestamp in seconds
    """

    tz_utc = pytz.timezone('UTC')
    tz_local = pytz.timezone(settings.TIME_ZONE)

    dt_naive = datetime.datetime.utcfromtimestamp(ts)  # no tz
    dt_utc = dt_naive.replace(tzinfo=tz_utc)
    dt_local = dt_utc.astimezone(tz_local)

    resdt_naive = dt_local.replace(tzinfo=None) - datetime.timedelta(days=days)
    resdt_local = tz_local.localize(resdt_naive)
    resdt_utc = resdt_local.astimezone(tz_utc)

    log.info('[AtsdReader] ' + str(dt_local) + ' - ' + str(days) + 'days = ' + str(resdt_local))

    return calendar.timegm(resdt_utc.timetuple())


def _time_minus_interval(end_time, interval):
    """substract given interval from end_time

    :param end_time: `Number` timestamp in seconds
    :param interval: {count: `Number`, unit: `str`}
    :return: `Number` timestamp in seconds
    """

    if interval['unit'] == 'MILLISECOND':
        seconds = interval['count'] / 1000.0
    elif interval['unit'] == 'SECOND':
        seconds = interval['count']
    elif interval['unit'] == 'MINUTE':
        seconds = interval['count'] * 60
    elif interval['unit'] == 'HOUR':
        seconds = interval['count'] * 60 * 60
    elif interval['unit'] == 'DAY':

        return _time_minus_days(end_time, interval['count'])

    elif interval['unit'] == 'WEEK':

        return _time_minus_days(end_time, interval['count'] * 7)

    elif interval['unit'] == 'MONTH':

        return _time_minus_months(end_time, interval['count'])

    elif interval['unit'] == 'QUARTER':

        return _time_minus_months(end_time, interval['count'] * 3)

    elif interval['unit'] == 'YEAR':

        return _time_minus_months(end_time, interval['count'] * 12)

    else:
        raise ValueError('wrong interval unit')

    return end_time - seconds


def _round_step(step):
    """example: 5003 -> 5000
    """
    # TODO: add logic
    return step


def _median_delta(values):
    """get array of delta, find median value
    values should be sorted
    values should contains at least two value

    :param values: `list` of `Numbers`
    :return: `Number`
    """

    deltas = []
    for i in range(1, len(values)):
        deltas.append(values[i] - values[i - 1])

    deltas.sort()
    return deltas[len(deltas) // 2]


def _regularize(series):
    """create values with equal periods

    :param series: should contains at least one value
    :return: time_info, values
    """

    # for sample in series:
    #     print(sample)
    times = [sample['t'] / 1000.0 for sample in series]
    step = _median_delta(times)
    step = _round_step(step)

    # round to divisible by step
    start_time = ((series[0]['t'] / 1000.0) // step) * step
    end_time = ((series[-1]['t'] / 1000.0) // step + 1) * step

    log.info('[AtsdReader] reqularize {0}:{1}:{2}'
             .format(start_time, step, end_time))
    number_points = int((end_time - start_time) // step)

    values = []
    sample_counter = 0

    for i in range(number_points):
        # on each step add some value
        if sample_counter > len(series) - 1:
            values.append(None)
            continue

        t = (start_time + i * step)
        sample = series[sample_counter]

        if abs(times[sample_counter] - t) <= step:
            values.append(sample['v'])
            sample_counter += 1
        else:
            values.append(None)

    time_info = (start_time,
                 start_time + number_points * step,
                 step)

    return time_info, values


def _str_to_interval(val):
    """
    :param val: `str` time interval
    :return: interval `tuple` (count, unit)
    """
    unit = val[-1]
    num = val[:-1]
    if unit == 's':
        return float(num), 'SECOND'
    elif unit == 'm':
        return float(num) * 60, 'SECOND'
    elif unit == 'h':
        return float(num) * 60 * 60, 'SECOND'
    elif unit == 'd':
        return float(num) * 24 * 60 * 60, 'SECOND'
    elif unit == 'y':
        return float(num), 'YEAR'
    else:
        return float(val), 'SECOND'


class Aggregator(object):
    __slots__ = ('type', 'count', 'unit', 'interpolate')

    def __init__(self, type, count, unit='SECOND', interpolate='STEP'):
        # TODO: throw Exception if type == 'DETAIL'
        if not count:
            raise ValueError('Aggregator.count could not be ' + unicode(count))

        type = type.upper()
        unit = unit.upper()
        interpolate = interpolate.upper()

        if unit == 'MILLISECOND':
            count /= 1000.0
            unit = 'SECOND'
        elif unit == 'MINUTE':
            count *= 60
            unit = 'SECOND'
        elif unit == 'HOUR':
            count *= 60 * 60
            unit = 'SECOND'
        elif unit == 'DAY':
            count *= 60 * 60 * 24
            unit = 'SECOND'

        #: `Number`
        self.count = count
        #: `str`
        self.unit = unit

        #: `str`
        self.type = type
        #: `str`
        self.interpolate = interpolate

    def json(self):
        return {
            'type': self.type,
            'interval': {'count': self.count, 'unit': self.unit},
            'interpolate': self.interpolate
        }

    def __str__(self):
        return '<Aggregator type={0}, period={1} {2}>'.format(self.type,
                                                              self.count,
                                                              self.unit)


class IntervalSchema(object):
    # _map: interval -> step in seconds

    __slots__ = ('_map',)

    CONF_NAME = os.path.join(settings.CONF_DIR, 'interval-schema.conf')

    _config = ConfigParser.RawConfigParser()
    _config.read(CONF_NAME)
    log.info('[IntervalSchema] sections=' + str(_config.sections()))

    def __init__(self, metric):
        """
        :param metric: `str` metric name
        """

        def section_matches(section):

            if self._config.has_option(section, 'metric-pattern'):
                metric_pattern = self._config.get(section, 'metric-pattern')
                return fnmatch.fnmatch(metric, metric_pattern)
            return True

        for section in self._config.sections():
            if section_matches(section):

                try:
                    # intervals has form 'x:y, z:t'
                    intervals = self._config.get(section, 'retentions')  # str
                    items = re.split('\s*,\s*', intervals)  # list of str
                    pairs = (item.split(':') for item in items)  # str tuples
                    self._map = dict((_str_to_interval(b), _str_to_interval(a))
                                     for a, b in pairs)
                    return

                except Exception as e:
                    log.exception('could not parse section {:s} in {:s}'
                                  .format(section, IntervalSchema.CONF_NAME), e)

        self._map = {}

    def aggregator(self, end_time, start_time, interval):
        """find step for current interval using interval schema

        return step of lowest schema interval that bigger than current
        if interval exists do not use start_time

        :param interval: `Number` seconds
        :return: :class:`.Aggregator` | None
        """

        if interval:
            start_time = _time_minus_interval(end_time, interval)

        period_map = {}  # interval_start -> period
        starts = []
        for count, unit in self._map:
            interval = {'count': count, 'unit': unit}
            interval_start = _time_minus_interval(end_time, interval)
            starts.append(interval_start)
            period_map[interval_start] = self._map[(count, unit)]

        starts.sort(reverse=True)

        count = 0
        unit = None
        for start in starts:
            print 'aggregator', period_map[start]
            if start <= start_time:
                count, unit = period_map[start]
                break

        if count:
            return Aggregator('AVG', count, unit)
        else:  # unit = None
            return None


class Instance(object):
    """
    series unique identifier
    """

    __slots__ = ('entity_name', 'metric_name', 'tags', '_client')

    def __init__(self, entity_name, metric_name, tags, client):
        #: `str`
        self.entity_name = entity_name
        #: `str`
        self.metric_name = metric_name
        #: `dict`
        self.tags = tags
        #: :class:`.AtsdClient`
        self._client = client

    def get_retention_interval(self):
        """
        :return: `Number` seconds or `None` if no default interval
        """
        try:
            return self._client.metric_intervals[self.metric_name]
        except KeyError:
            return None

    def fetch_series(self, start_time, end_time, aggregator):
        """
        :param start_time: `Number` seconds
        :param end_time: `Number` seconds
        :param aggregator: :class:`.Aggregator` | None
        :return: :class: `.FetchInProgress` <(start, end, step), [values]>
        """

        future = self._client.query_series(self, start_time, end_time, aggregator)

        def get_formatted_series():
            resp = future.waitForResults()
            series = resp['data']

            if not len(series):
                return (start_time, end_time, end_time - start_time), [None]
            if len(series) == 1:
                return (start_time, end_time, end_time - start_time), [series[0]['v']]

            if aggregator and aggregator.unit == 'SECOND':
                # data regularized, send as is
                time_info = (float(series[0]['t']) / 1000,
                             float(series[-1]['t']) / 1000 + aggregator.count,
                             aggregator.count)

                values = [sample['v'] for sample in series]
            else:
                time_info, values = _regularize(series)

            log.info('[reader.Instance] fetched {0} values, time_info={1}:{2}:{3}'
                     .format(len(values), time_info[0], time_info[2], time_info[1]))

            return time_info, values

        return FetchInProgress(get_formatted_series)

    def get_metric(self):
        """make meta api request

        :return: parsed json response
        """

        return self._client.request('GET',
                                    'metrics/' + utils.quote(self.metric_name))

    def get_entity(self):
        """make meta api request

        :return: parsed json response
        """

        return self._client.request('GET',
                                    'entities/' + utils.quote(self.entity_name))


# noinspection PyMethodMayBeStatic
class EmptyReader(object):

    def fetch(self, start, end):
        raise RuntimeError('empty reader could not fetch')

    def get_intervals(self):
        return IntervalSet([Interval(0, 1)])


class AtsdReader(object):
    __slots__ = ('_instance',
                 'aggregator',
                 '_interval_schema',
                 'default_interval',
                 '_pid')

    def __init__(self, client, entity, metric, tags, default_interval=None,
                 aggregator=None):

        #: :class: `.Node`
        self._instance = Instance(entity, metric, tags, client)

        if aggregator and aggregator.type == 'DETAIL':
            aggregator = None

        #: :class: `.Aggregator` | `None`
        self.aggregator = aggregator

        #: :class:`.IntervalSchema`
        self._interval_schema = IntervalSchema(metric)

        if default_interval:
            default_interval['unit'] = default_interval['unit'].upper()
        #: {unit: `str`, count: `Number`} | None
        self.default_interval = default_interval

        #: `str` process info
        self._pid = str(os.getpid())

        log.info('[AtsdReader] init: entity=' + unicode(entity)
                 + ' metric=' + unicode(metric)
                 + ' tags=' + unicode(tags)
                 + ' url=' + unicode(settings.ATSD_CONF['url'])
                 + ' aggregator=' + unicode(aggregator)
                 + ' interval=' + unicode(default_interval))

    def fetch(self, start_time, end_time):
        """fetch time series

        :param start_time: `Number` seconds
        :param end_time: `Number` seconds
        :return: :class:`.FetchInProgress`
        """

        if self.default_interval:
            start_time = _time_minus_interval(end_time, self.default_interval)

        log.info(
            '[AtsdReader {2}] fetching: interval=({0}, {1})'
            .format(strf_timestamp(start_time), strf_timestamp(end_time), self._pid)
        )

        if self.aggregator:
            aggregator = self.aggregator
        else:
            aggregator = self._interval_schema.aggregator(end_time, start_time,
                                                          self.default_interval)

        return self._instance.fetch_series(start_time, end_time, aggregator)

    def get_intervals(self):
        """
        :return: :class:`.IntervalSet`
        """

        retention_interval = self._instance.get_retention_interval()
        if retention_interval is not None:
            now = time.time()
            if retention_interval == 0:
                start_time = 0
            else:
                start_time = now - retention_interval

            log.info('[AtsdReader ' + self._pid + ']'
                     + ' default retention_interval=('
                     + strf_timestamp(start_time) + ','
                     + strf_timestamp(now) + ')')

            return IntervalSet([Interval(start_time, now)])

        # FIXME: metric not available in tests
        metric = self._instance.get_metric()
        try:
            entity = self._instance.get_entity()
        except RuntimeError:  # server response != 200
            end_time = metric['lastInsertTime'] / 1000
        else:
            end_time = max(metric['lastInsertTime'], entity['lastInsertTime']) / 1000

        retention = metric['retentionInterval'] * 24 * 60 * 60
        start_time = (end_time - retention) if retention else 1

        log.info('[AtsdReader ' + self._pid + ']'
                 + ' retention_interval=('
                 + strf_timestamp(start_time) + ','
                 + strf_timestamp(end_time) + ')')

        return IntervalSet([Interval(start_time, end_time)])
