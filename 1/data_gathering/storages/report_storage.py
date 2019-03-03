# -*- coding: utf-8 -*-
import abc


class ReportStorage(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def write_history_report(self):
        raise NotImplementedError

    @abc.abstractmethod
    def write_current_report(self, df):
        raise NotImplementedError

    @abc.abstractmethod
    def write_tickers_report(self, tic_name):
        raise NotImplementedError

    @abc.abstractmethod
    def check_history_report(self):
        raise NotImplementedError

    @abc.abstractmethod
    def check_current_report(self, df):
        raise NotImplementedError

    @abc.abstractmethod
    def check_tickers_report(self, tic_list):
        raise NotImplementedError

