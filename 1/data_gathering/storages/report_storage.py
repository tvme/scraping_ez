# -*- coding: utf-8 -*-
import abc


class ReportStorage(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def write_report(self, tic_dict):
        raise NotImplementedError

    @abc.abstractmethod
    def check_report(self, tic_list, func_nm, start_dt):
        raise NotImplementedError

