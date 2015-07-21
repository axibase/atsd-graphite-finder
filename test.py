import unittest
import time
import atsd_finder


class TestAtsdFinder(unittest.TestCase):
    def test_interval_schema(self):
        reader = atsd_finder.AtsdReader('nurswgvml006',
                                        'cpu_busy',
                                        tags={},
                                        step=0,
                                        statistic='DETAIL')
        time_info, values = reader.fetch(time.time() - 2 * 24 * 60 * 60, time.time())
        print time_info

    def test_reader_fetch(self):
        reader = atsd_finder.AtsdReader('atsd',
                                        'metric_gets_per_second',
                                        {'host': 'NURSWGVML007'},
                                        5, 'AVG')
        time_info, values = reader.fetch(time.time() - 24 * 60 * 60, time.time())

        start = time_info[0]
        end = time_info[1]
        step = time_info[2]
        self.assertEqual((end - start) / step, float(len(values)))

    def test_reader_group(self):
        reader = atsd_finder.AtsdReader('nurswgvml006',
                                        'cpu_busy',
                                        tags={},
                                        step=60*60,
                                        statistic='MIN')
        time_info_min, values_min = reader.fetch(time.time() - 60 * 60, time.time())

        reader = atsd_finder.AtsdReader('nurswgvml006',
                                        'cpu_busy',
                                        tags={},
                                        step=60*60,
                                        statistic='MAX')
        time_info_max, values_max = reader.fetch(time.time() - 2 * 60 * 60, time.time())

        value_min = values_min[-1]
        if time_info_min[1] == time_info_max[1]:
            value_max = values_max[-1]
        else:
            value_max = values_max[-2]

        self.assertGreater(value_max, value_min)

    def test_reader_fetch_raw(self):
        reader = atsd_finder.AtsdReader('safeway',
                                        'retail_price',
                                        {u'category': u'Breakfast-Cereal/Cereal/Cereal--All-Family',
                                         u'name': u'Cheerios Cereal - 18 Oz',
                                         u'currency': u'usd',
                                         u'quantity': u'18.0',
                                         u'unit': u'oz',
                                         u'zip_code': u'20032'},
                                        0, 'AVG')
        time_info, values = reader.fetch(time.time() - 24 * 60 * 60, time.time())

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