# -*- coding: utf-8 -*-
import abc


class DatedStorage(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def read_dated_df(self):
        raise NotImplementedError

    @abc.abstractmethod
    def write_dated_df(self, df):
        raise NotImplementedError
