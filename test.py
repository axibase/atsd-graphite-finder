import unittest
import time
import atsd_finder


class TestAtsdFinder(unittest.TestCase):
    def test_interval_schema(self):
        now = time.time()
        reader = atsd_finder.AtsdReader('nurswgvml006',
                                        'cpu_busy',
                                        tags={},
                                        step=0,
                                        aggregator='DETAIL')
        time_info_day, vals_day = reader.fetch(now - 2 * 60 * 60, now)
        time_info_hour, _ = reader.fetch(now - 60 * 60, now)

        self.assertGreater(time_info_day[2], time_info_hour[2])

        reader_group = atsd_finder.AtsdReader('nurswgvml006',
                                        'cpu_busy',
                                        tags={},
                                        step=60,
                                        aggregator='AVG')
        time_info_group, vals_group = reader_group.fetch(now - 2 * 60 * 60, now)

        self.assertListEqual(vals_group, vals_day)

    def test_reader_fetch(self):
        now = time.time()
        reader = atsd_finder.AtsdReader('atsd',
                                        'metric_gets_per_second',
                                        {'host': 'NURSWGVML007'},
                                        5, 'AVG')
        time_info, values = reader.fetch(now - 24 * 60 * 60, now)

        start = time_info[0]
        end = time_info[1]
        step = time_info[2]
        self.assertEqual((end - start) / step, float(len(values)))

    def test_reader_group(self):
        now = time.time()
        reader = atsd_finder.AtsdReader('nurswgvml006',
                                        'cpu_busy',
                                        tags={},
                                        step=60 * 60,
                                        aggregator='MIN')
        _, values_min = reader.fetch(now - 60 * 61, now)

        reader = atsd_finder.AtsdReader('nurswgvml006',
                                        'cpu_busy',
                                        tags={},
                                        step=60 * 60,
                                        aggregator='MAX')
        _, values_max = reader.fetch(now - 60 * 61, now)

        print values_max[0], values_min[0]
        self.assertGreater(values_max[0], values_min[0])

    def test_reader_fetch_raw(self):
        now = time.time()
        reader = atsd_finder.AtsdReader('safeway',
                                        'retail_price',
                                        {u'category': u'Breakfast-Cereal/Cereal/Cereal--All-Family',
                                         u'name': u'Cheerios Cereal - 18 Oz',
                                         u'currency': u'usd',
                                         u'quantity': u'18.0',
                                         u'unit': u'oz',
                                         u'zip_code': u'20032'},
                                        0, 'AVG')
        time_info, values = reader.fetch(now - 24 * 60 * 60, now)

        start = time_info[0]
        end = time_info[1]
        step = time_info[2]
        self.assertEqual((end - start) / step, float(len(values)))

    def test_reader_get_intervals(self):
        reader = atsd_finder.AtsdReader('atsd',
                                        'metric_gets_per_second',
                                        {'host': 'NURSWGVML007'},
                                        1, 'AVG')
        reader.get_intervals()

    def test_finder(self):
        atsd_finder.AtsdFinder()