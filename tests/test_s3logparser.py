#!/usr/bin/python
# Copyright 2015 Mykola Yakovliev <vegasq@gmail.com>

import datetime
import mock
import testtools

from s3logparser import s3logparser


class TestBasicEx(testtools.TestCase):
    def setUp(self):
        super(TestBasicEx, self).setUp()
        self.s3lp = s3logparser.S3LogParser('/etc', 'test.csv')

    def test_initial_values(self):
        self.assertIsInstance(self.s3lp, s3logparser.S3LogParser)
        self.assertEqual(self.s3lp.OUTPUT_FILE, 'test.csv')
        self.assertEqual(self.s3lp.LOG_DIR, '/etc')

    def test_logs_param(self):
        self.assertIsInstance(self.s3lp.logs, list)
        for item in self.s3lp.logs:
            self.assertIsInstance(item, str)

    def test_build_data(self):
        fake = mock.MagicMock(return_value=['fakeA', 'fakeB'])
        self.s3lp.build_data(fake)
        self.assertEqual(self.s3lp.data, ['fakeA', 'fakeB'])
        fake.assert_called_once_with([])

    def test_popular_files(self):
        data = [['', '', '', '', '', '', '', '', 'fake fake', '']]
        result = s3logparser.popular_files(data)
        self.assertEqual(result, {'fake': 1})
