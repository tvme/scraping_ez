# -*- coding: utf-8 -*-
import logging
from pathlib import Path
import sys
import os
from datetime import datetime
import pandas as pd

from storages.dated_df_storage import DatedDfStorage
from storages.report_storage import ReportStorage

DEFOULT_REPORT_COLUMNS_NM = 'date,logger_nm,tic_name,func_nm,status,description\n'
idx = pd.IndexSlice


class MyFormatter(logging.Formatter):
    '''
    time formatter without ',' for csv-file purpose
    '''
    converter = datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime('%Y-%m-%d %H:%M:%S')
            s = '%s.%03d' % (t, record.msecs)
        return s


def make_line(tic, func, status, descript):
    descript = descript.replace(',', ';')  # replace comma for csv-file record
    s = '{tic},{func},{status},{descript}'.format(tic=tic, func=func, status=status, descript=descript)
    return s


class ParseLogReport(ReportStorage):
    '''
    пишем отчет в файл report.csv
    date, logger_nm,tic_name, func_nm,  status['OK', 'ERROR'],  description: описание если есть ошибка
    в описание все ',' заменяем на ';'

    check возвращает dict[tic]:of booleans
    '''

    def __init__(self, report_name, col_nm=DEFOULT_REPORT_COLUMNS_NM):
        self.report_name = report_name
        if not Path(self.report_name).is_file():  # если файла нет, создаем 1-ю строку - имена столбцов
            with open(self.report_name, 'w') as report_file:
                report_file.write(col_nm)
        self.parse_logger = logging.getLogger('parse_logger')  # create logger to file, message without ',' for csv-file format
        self.parse_logger.setLevel(logging.INFO)
        fh = logging.FileHandler(self.report_name)
        formatter = MyFormatter(fmt='%(asctime)s,%(name)s,%(message)s')
        fh.setFormatter(formatter)
        self.parse_logger.addHandler(fh)
        self.parse_logger.propagate = False


    def write_report(self, log_tic_dict):  # dict like {'TIC':['func_nm', 'status', 'description']}
        for tic, str_list in log_tic_dict.items():
            func_nm, status, descript = str_list
            self.parse_logger.info(make_line(tic, func_nm, status, descript))


    def check_report(self, tic_list, func_nm, start_dt):
        '''
        проверяет отчет по списку tic_list для операции func_nm начина с start_dt
        :return: tic_status dict with tic as keys and True or False values
        '''
        df = pd.read_csv(self.report_name, index_col=['date', 'tic_name'], parse_dates=['date'])
        df = df[(df.index.get_level_values('date') >= pd.Timestamp(start_dt)) &
                (df.loc[:, 'func_nm'] == func_nm)]
        tic_status = {}
        for tic in tic_list:
            try:
                status_list = df.loc[idx[:, tic], 'status'].tolist()
                if 'OK' in status_list:
                    tic_status[tic] = True
                else:
                    tic_status[tic] = False
            except KeyError:
                tic_status[tic] = False
        return tic_status  # TODO: возможно вместо dict нужно использовать list



if __name__ == '__main__':
    # tic_fl_nm = 'C:\\Users\\ersan\\PycharmProjects\\DS_OTUS\\1\\data_gathering\\data\\tickers\\2019-02-06_tickers_test.csv'
    report_fl_nm = 'C:\\Users\\ersan\\PycharmProjects\\DS_OTUS\\1\\data_gathering\\data\\parse_test_report.csv'
    date_tickers_parsing = datetime.strptime('2019-02-06', '%Y-%m-%d')
    tickers_storage = DatedDfStorage(date=date_tickers_parsing,
                                     suffix='tickers_test',  # суфикс для тестового файла
                                     dir_name='C:\\Users\\ersan\\PycharmProjects\\DS_OTUS\\1\\data_gathering\\data\\tickers')
    tickers_df = tickers_storage.read_dated_df(index_col='tic')
    log_report = ParseLogReport(report_name=report_fl_nm)
    tic_dict = {tic: ['tic_load', 'OK', ''] for tic in tickers_df.index.tolist()}
    tic_dict['AAA'] = ['tic_load', 'ERROR', 'test err, log']
    log_report.write_report(log_tic_dict=tic_dict)
    # print(log_report.check_tickers_report(tic_list=tic_list, start_dt=pd.Timestamp('2019-03-16 19:10:00')))
