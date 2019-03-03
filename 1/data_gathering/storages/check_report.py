# -*- coding: utf-8 -*-
import logging
import sys
import os
import ntpath
import datetime
import pandas as pd

from storages.dated_df_storage import DatedDfStorage
from storages.report_storage import ReportStorage


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
idx = pd.IndexSlice


class CheckReport(ReportStorage):
    '''

    '''
    def __init__(self, report_name):
        self.report_name = report_name

    def write_history_report(self):
        raise NotImplementedError

    def write_current_report(self, data_name):
        fl_date = pd.Timestamp(ntpath.basename(data_name)[:10]).date()  # date from file name
        data_df = pd.read_csv(data_name, index_col=['metric', 'calendar_qrt', 'sources'])
        index = pd.MultiIndex.from_tuples([(fl_date, 'cur_rep')], names=['date', 'report'])
        report_df = pd.DataFrame(index=index, columns=tic_df['tic'])


    def write_tickers_report(self, tic_name):
        fl_date = pd.Timestamp(ntpath.basename(tic_name)[:10]).date()  # date fronm file name
        tic_df = pd.read_csv(tic_name)
        index = pd.MultiIndex.from_tuples([(fl_date, 'tic_rep')], names=['date', 'report'])
        report_df = pd.DataFrame(index=index, columns=tic_df['tic'])
        report_df.loc[(fl_date, 'tic_rep')] = True
        if not os.path.exists(self.report_name):  # самый первый отчет
            # iterables = [[date] , ['tic_rep', 'hyst_rep', 'cur_rep']]
            # index = pd.MultiIndex.from_product(iterables, names=['date', 'report'])
            report_df.to_csv(self.report_name)
            logger.info("Created report file: {f_n}".format(f_n=self.report_name))
        else:
            df_list = [pd.read_csv(self.report_name, index_col=['date', 'report'], parse_dates=True)]
            df_list.append(report_df)
            df = pd.concat(df_list, sort=True)
            df = df[~df.index.duplicated(keep='last')]  # удаляе дубли по индексу, оставляем последюю запись за день
            df.sort_index(axis=0, level='date', ascending=False, inplace=True)  # last report on the top
            df.to_csv(self.report_name)
            logger.info("Add ticker report with date:{dt} to file: {f_n}".format(dt=fl_date, f_n=self.report_name))


    def check_history_report(self):
        raise NotImplementedError

    def check_current_report(self, df):
        raise NotImplementedError

    def check_tickers_report(self, tic_list):
        '''
        проверяет отчет по списку тикеров
        :return: Pandas serie: index=tic_list, value=last date record or NaN
        '''
        df = pd.read_csv(self.report_name, index_col=['date', 'report'], parse_dates=True)
        s = pd.Series()
        tic_list = ['AAN', 'AAPL', 'AEO', 'CF']
        for tic in tic_list:
            try:
                tic_s = df.loc[idx[:, 'tic_rep'], tic].reset_index(level='report', drop=True)
                tic_s = tic_s[tic_s == True]
                if len(tic_s) != 0:
                    s[tic] = tic_s.index[0].date()
                else:
                    s[tic] = None
            except KeyError:
                s[tic] = None
        return s



if __name__ == '__main__':
    tic_fl_nm = 'C:\\Users\\ersan\\PycharmProjects\\DS_OTUS\\1\\data_gathering\\data\\tickers\\2019-02-06_tickers_test.csv'
    report_fl_nm = 'C:\\Users\\ersan\\PycharmProjects\\DS_OTUS\\1\\data_gathering\\data\\report.csv'
    checker = CheckReport(report_name=report_fl_nm)
    checker.write_tickers_report(tic_name=tic_fl_nm)
