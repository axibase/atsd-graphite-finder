import unittest
import time
import atsd_finder


class TestAtsdFinder(unittest.TestCase):

    def test_reader_fetch(self):
        reader = atsd_finder.AtsdReader('atsd',
                                        'metric_gets_per_second',
                                        {'host': 'NURSWGVML007'},
                                        5)
        res = reader.fetch(time.time() - 24 * 60 * 60, time.time())

        start = res[0][0]
        end = res[0][1]
        step = res[0][2]
        self.assertEqual((end - start) / step, float(len(res[1])))

    def test_reader_fetch_raw(self):
        reader = atsd_finder.AtsdReader('safeway',
                                        'retail_price',
                                        {u'category': u'Breakfast-Cereal/Cereal/Cereal--All-Family',
                                         u'name': u'Cheerios Cereal - 18 Oz',
                                         u'currency': u'usd',
                                         u'quantity': u'18.0',
                                         u'unit': u'oz',
                                         u'zip_code': u'20032'},
                                        0)
        res = reader.fetch(time.time() - 24 * 60 * 60, time.time())

        start = res[0][0]
        end = res[0][1]
        step = res[0][2]
        self.assertEqual((end - start) / step, float(len(res[1])))

    def test_reader_get_intervals(self):
        reader = atsd_finder.AtsdReader('atsd',
                                        'metric_gets_per_second',
                                        {'host': 'NURSWGVML007'},
                                        1)
        reader.get_intervals()

    def test_finder(self):
        finder = atsd_finder.AtsdFinder()