import ConfigParser
import fnmatch
import re
import os
import datetime
import calendar
import pytz

from . import utils
from graphite.intervals import Interval, IntervalSet

log = utils.get_logger()

try:
    # noinspection PyUnresolvedReferences
    from django.conf import settings
    from graphite.readers import FetchInProgress
except:  # debug env
    from graphite import settings
    from sample import FetchInProgress
    log.info('reader running in debug environment', 'AtsdReader')


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

    log.info(str(dt_local) + ' - ' + str(days) + 'days = ' + str(resdt_local), 'AtsdReader')

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


def _regularize(series, step=None):
    """create values with equal periods

    :param step: `Number` seconds
    :param series: `[{t: long, v: float}]` should contains at least one value
    :return: time_info, values
    """

    # for sample in series:
    #     print(sample)
    times = [sample['t'] / 1000.0 for sample in series]

    if step is None:
        step = _median_delta(times)
        step = _round_step(step)

    # round to divisible by step
    start_time = ((series[0]['t'] / 1000.0) // step) * step
    end_time = ((series[-1]['t'] / 1000.0) // step + 1) * step

    # log.info('regularize {0}:{1}:{2}'
    #          .format(start_time, step, end_time), 'AtsdReader')

    number_points = int(round((end_time - start_time) / step))

    values = []
    sample_counter = 0

    for i in range(number_points):
        # on each step add some value

        if sample_counter > len(series) - 1:
            values.append(None)
            continue

        t = (start_time + i * step)
        sample = series[sample_counter]

        if abs(times[sample_counter] - t) < step:
            values.append(sample['v'])
            sample_counter += 1
        else:
            values.append(None)

    time_info = (start_time,
                 start_time + number_points * step,
                 step)

    return time_info, values


class Aggregator(object):
    __slots__ = ('type', 'count', 'unit', 'interpolate')

    def __init__(self, type, count, unit='SECOND', interpolate='STEP'):
        """type=DETAIL is used to prevent data regularization, use with care
        """

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
        return '<Aggregator type={0}, period={1}-{2}>'.format(self.type,
                                                              self.count,
                                                              self.unit)


class IntervalSchema(object):
    # _map: interval `(count, unit)` -> `.Aggregator` | None

    __slots__ = ('_map',)

    CONF_NAME = os.path.join(settings.CONF_DIR, 'interval-schema.conf')

    _config = ConfigParser.RawConfigParser()
    _config.read(CONF_NAME)
    log.info('sections=' + str(_config.sections()), 'IntervalSchema')

    def __init__(self, path):
        """
        :param path: `str` metric name
        """

        def section_matches(section_):

            if self._config.has_option(section_, 'metric-pattern'):
                metric_pattern = self._config.get(section_, 'metric-pattern')
                return fnmatch.fnmatch(path, metric_pattern)
            return True

        for section in self._config.sections():
            if section_matches(section):

                try:
                    # intervals has form 'x:y, z:t'
                    intervals = self._config.get(section, 'retentions')  # str
                    items = re.split('\s*,\s*', intervals)  # list of str

                    self._map = {}
                    for item in items:
                        tokens = item.split(':')

                        interval = _str_to_interval(tokens[1])
                        step_count, step_unit = _str_to_interval(tokens[0])
                        type = tokens[2].upper() if len(tokens) == 3 else 'AVG'

                        if step_count == 0:
                            self._map[interval] = None
                        else:
                            self._map[interval] = Aggregator(type, step_count, step_unit)

                    return

                except Exception as e:
                    log.exception('could not parse section {:s} in {:s}'
                                  .format(section, IntervalSchema.CONF_NAME)
                                  + unicode(e), self)

        self._map = {}

    def aggregator(self, end_time, start_time, interval):
        """find step for current interval using interval schema

        return step of the widest schema interval that include start_time
        if interval exists do not use start_time

        :param start_time: `Number` timestamp in seconds
        :param end_time: `Number` timestamp in seconds
        :param interval: `Number` seconds
        :return: :class:`.Aggregator` | None
        """

        if interval:
            start_time = _time_minus_interval(end_time, interval)

        starts_map = {}  # interval_start -> aggregator
        starts = []
        for count, unit in self._map:
            interval = {'count': count, 'unit': unit}
            interval_start = _time_minus_interval(end_time, interval)
            starts.append(interval_start)
            starts_map[interval_start] = self._map[(count, unit)]

        if len(starts) == 0:
            return None

        starts.sort(reverse=True)

        for start in starts:
            if start <= start_time:
                return starts_map[start]

        # noinspection PyUnboundLocalVariable
        # starts not empty
        return starts_map[start]


# noinspection PyMethodMayBeStatic
class EmptyReader(object):
    __slots__ = ()

    def fetch(self, start, end):
        raise RuntimeError('empty reader could not fetch')

    def get_intervals(self):
        return IntervalSet([Interval(0, 1)])


class AtsdReader(object):
    __slots__ = ('_instance',
                 'aggregator',
                 '_interval_schema',
                 'default_interval')

    def __init__(self, instance, default_interval=None, aggregator=None):
        """CAUTION: aggregator.type DETAIL is used to prevent data regularization
        """

        #: :class: `.Node`
        self._instance = instance

        #: :class: `.Aggregator` | `None`
        self.aggregator = aggregator

        #: :class:`.IntervalSchema`
        self._interval_schema = IntervalSchema(instance.path)

        if default_interval:
            default_interval['unit'] = default_interval['unit'].upper()
        #: {unit: `str`, count: `Number`} | None
        self.default_interval = default_interval

        log.info('init: entity=' + unicode(instance.entity_name)
                 + ' metric=' + unicode(instance.metric_name)
                 + ' tags=' + unicode(instance.tags)
                 + ' aggregator=' + unicode(aggregator)
                 + ' interval=' + unicode(default_interval),
                 'AtsdReader:' + str(id(self)))

    def fetch(self, start_time, end_time):
        """fetch time series

        :param start_time: `Number` seconds
        :param end_time: `Number` seconds
        :return: :class:`.FetchInProgress`
        """

        if self.default_interval:
            start_time = _time_minus_interval(end_time, self.default_interval)

        # log.info('fetching: interval=({0} - {1})'
        #          .format(strf_timestamp(start_time), strf_timestamp(end_time)),
        #          self)

        if self.aggregator:
            aggregator = self.aggregator
        else:
            aggregator = self._interval_schema.aggregator(end_time, start_time,
                                                          self.default_interval)

        def format_series(series):
            """
            :param series: [{t, v}]
            :return: (start, end, step), [values]
            """

            if not len(series):
                time_info = start_time, end_time, end_time - start_time
                values = [None]

            elif len(series) == 1:
                time_info = start_time, end_time, end_time - start_time
                values = [series[0]['v']]

            elif aggregator and aggregator.unit == 'SECOND':
                step = aggregator.count
                start = series[0]['t'] / 1000.0
                end = series[-1]['t'] / 1000.0 + step

                if (end - start) / step == len(series):
                    time_info = start, end, step
                    values = [s['v'] for s in series]
                else:
                    time_info, values = _regularize(series, step)

            else:
                time_info, values = _regularize(series)

            log.info('fetched {0} samples, interval={1} - {2}, step={3}sec'
                     .format(len(series),
                             utils.strf_timestamp(time_info[0]),
                             utils.strf_timestamp(time_info[1]),
                             time_info[2]),
                     'AtsdReader:' + str(id(self)))

            return time_info, values

        return self._instance.fetch_series(start_time, end_time, aggregator, format_series)

    def get_intervals(self):
        """
        :return: :class:`.IntervalSet`
        """

        start_time, end_time = self._instance.get_retention_interval()

        return IntervalSet([Interval(start_time, end_time)])
