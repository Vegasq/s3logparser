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
    def logs(self):
        """
        Should return list of lg files with some rules
        """
        return ["%s%s" %  (self.LOG_DIR, log) for log in self.log_files]

    def build_data(self, do):
        """Parse simple log parser"""
        for log_file in self.logs:
            try:
                rows = open(log_file, 'r').readlines()
                for row in rows:
                    self.data.append(self.REGEX.findall(row)[0])
            except (IOError, IndexError):
                continue
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
