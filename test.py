import unittest
import time
import atsd_finder
from atsd_finder.reader import Aggregator
from atsd_finder.client import Client


class FetchInProgress(object):
    def __init__(self, wait_callback):
        self.wait_callback = wait_callback

    def waitForResults(self):
        return self.wait_callback()


class TestReaderFetch(unittest.TestCase):
    client = Client()

    def test_interval_schema(self):
        now = time.time()
        reader = atsd_finder.AtsdReader(self.client,
                                        'nurswgvml006',
                                        'cpu_busy',
                                        tags={})
        time_info_day, vals_day = reader.fetch(now - 2 * 60 * 60, now).waitForResults()
        time_info_hour, _ = reader.fetch(now - 60 * 60, now).waitForResults()

        self.assertGreater(time_info_day[2], time_info_hour[2])

        reader_group = atsd_finder.AtsdReader(self.client,
                                              'nurswgvml006',
                                              'cpu_busy',
                                              {},
                                              aggregator=Aggregator('AVG', 60, 'SECOND'))
        time_info_group, vals_group = reader_group.fetch(now - 2 * 60 * 60, now).waitForResults()

        self.assertListEqual(vals_group, vals_day)

    def test_reader_fetch_tags(self):
        now = time.time()
        reader = atsd_finder.AtsdReader(self.client,
                                        'atsd',
                                        'metric_gets_per_second',
                                        {'host': 'NURSWGVML007'},
                                        aggregator=Aggregator('AVG', 5, 'SECOND'))
        time_info, values = reader.fetch(now - 24 * 60 * 60, now).waitForResults()

        start = time_info[0]
        end = time_info[1]
        step = time_info[2]
        self.assertEqual((end - start) / step, float(len(values)))

    def test_reader_fetch_wildcard_aggregate(self):
        now = time.time()
        reader = atsd_finder.AtsdReader(self.client,
                                        'nurswgvml*',
                                        'cpu_busy',
                                        {},
                                        aggregator=Aggregator('DELTA', 5, 'SECOND'))
        time_info, values = reader.fetch(now - 24 * 60 * 60, now).waitForResults()

        start = time_info[0]
        end = time_info[1]
        step = time_info[2]
        self.assertEqual((end - start) / step, float(len(values)))

    def test_reader_group(self):
        now = time.time()
        reader = atsd_finder.AtsdReader(self.client,
                                        'nurswgvml006',
                                        'cpu_busy',
                                        {},
                                        aggregator=Aggregator('MIN', 60 * 60, 'SECOND'))
        _, values_min = reader.fetch(now - 60 * 61, now).waitForResults()

        reader = atsd_finder.AtsdReader(self.client,
                                        'nurswgvml006',
                                        'cpu_busy',
                                        {},
                                        aggregator=Aggregator('MAX', 60 * 60, 'SECOND'))
        _, values_max = reader.fetch(now - 60 * 61, now).waitForResults()

        print values_max[0], values_min[0]
        self.assertGreater(values_max[0], values_min[0])

    def test_reader_fetch_raw(self):
        now = time.time()
        reader = atsd_finder.AtsdReader(self.client,
                                        'safeway',
                                        'retail_price',
                                        {u'category': u'Breakfast-Cereal/Cereal/Cereal--All-Family',
                                         u'name': u'Cheerios Cereal - 18 Oz',
                                         u'currency': u'usd',
                                         u'quantity': u'18.0',
                                         u'unit': u'oz',
                                         u'zip_code': u'20032'})
        time_info, values = reader.fetch(now - 24 * 60 * 60, now).waitForResults()

        start = time_info[0]
        end = time_info[1]
        step = time_info[2]
        self.assertEqual((end - start) / step, float(len(values)))

    def test_fetch_default_interval(self):
        now = time.time()
        reader = atsd_finder.AtsdReader(self.client,
                                        'atsd',
                                        'metric_gets_per_second',
                                        {'host': 'NURSWGVML007'},
                                        default_interval={'unit': 'SECOND', 'count': 60})
        time_info, values = reader.fetch(now - 24 * 60 * 60, now).waitForResults()
        self.assertGreater(60 + time_info[2], time_info[1] - time_info[0])


class TestReader(unittest.TestCase):
    client = Client()

    def test_get_intervals(self):
        reader = atsd_finder.AtsdReader(self.client,
                                        'atsd',
                                        'metric_gets_per_second',
                                        {'host': 'NURSWGVML007'},
                                        aggregator=Aggregator('AVG', 1, 'SECOND'))
        reader.get_intervals()

    def test_aggregator(self):
        aggregator = Aggregator('AVG', 1, 'DAY')
        serialized = aggregator.json()

        self.assertEqual('AVG', serialized['type'])

    def test_aggregator_zero_interval(self):
        with self.assertRaises(ValueError):
            Aggregator('AVG', 0, 'SECOND')

    def test_reader_interval_schema(self):
        schema = atsd_finder.reader.IntervalSchema('cpu_busy')

        aggregator = schema.aggregator(2 * 60 * 60, 1 * 60 * 60, None)
        self.assertIsNone(aggregator)

        # aggregator = schema.aggregator(60 * 60 * 24 * 20 + 1)
        # self.assertIsInstance(aggregator, Aggregator)
        # self.assertEqual(aggregator.count, 1)
        # self.assertEqual(aggregator.unit, 'DAY')


class TestFinder(unittest.TestCase):

    def test_finder(self):
        atsd_finder.AtsdFinder()

    def test_finderV(self):
        atsd_finder.AtsdFinderV()


class TestClient(unittest.TestCase):

    def test_request(self):
        client = Client()
        resp = client.request('GET', 'metrics/cpu_busy')
        self.assertEqual(resp['name'], 'cpu_busy')