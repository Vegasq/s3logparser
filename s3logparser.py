#!/usr/bin/python
# Copyright 2015 Mykola Yakovliev <vegasq@gmail.com>

import csv
import os
import sys
import re


class S3LogParser(object):
    """
    Parse log files provided by Amazon S3 service.
    Basicly I use it to understand what is the mostpopular files.
    """

    LOG_DIR = 'lgs/logs/'
    REGEX = re.compile(
        r'(?P<owner>\S+) (?P<bucket>\S+) (?P<time>\[[^]]*\]) (?P<ip>\S+) ' + 
        r'(?P<requester>\S+) (?P<reqid>\S+) (?P<operation>\S+) ' +
        r'(?P<key>\S+) (?P<request>"[^"]*") (?P<status>\S+) (?P<error>\S+) ' +
        r'(?P<bytes>\S+) (?P<size>\S+) (?P<totaltime>\S+) ' +
        r'(?P<turnaround>\S+) (?P<referrer>"[^"]*") (?P<useragent>"[^"]*") ' +
        r'(?P<version>\S)')

    def __init__(self, log_dir=None, output_file=None):
        """Initial data"""
        if log_dir:
            self.LOG_DIR = log_dir
        if output_file:
            self.OUTPUT_FILE = output_file
        self.log_files = os.listdir(self.LOG_DIR)
        self.data = []

    @property
    def logs(self, date_from=None, date_to=None):
        """
        Should return list of lg files with some rules
        """
        if date_from and date_to:
            pass
        elif date_to:
            pass
        elif date_from:
            pass

        return ["%s%s" %  (self.LOG_DIR, log) \
            for log in self._filter_log_files(self.log_files)]

    def _filter_log_files(self, logs):
        """Cut log files based on their  filenames"""
        return logs

    def _filter_log_rows(self, logs):
        """Cur log rows"""
        return logs

    def build_data(self, do):
        """Parse simple log parser"""
        for log_file in self.logs:
            rows = open(log_file, 'r').readlines()
            for row in rows:
                try:
                    self.data.append(self.REGEX.findall(row)[0])
                except IndexError:
                    pass
        self.data = do(self.data)

    def to_csv(self):
        with open(self.OUTPUT_FILE, 'wb') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            for line in self.data:
                writer.writerow([line, self.data[line]])


def popular_files(data):
    response = {}
    for row in data:
        line = {
            'date': row[2],
            'ip': row[3],
            'url': row[8].split(" ")[1]
        }
        if line['url'] not in response:
            response[line['url']] = 0
        response[line['url']] += 1
    return response
