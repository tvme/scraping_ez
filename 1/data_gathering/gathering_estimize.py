# -*- coding: utf-8 -*-
import logging
import sys
import argparse
import os
import time
import datetime
import pandas as pd

from parsers.parse_tickers import get_tickers_df
from parsers.parse_data import get_history_data
from storages.dated_df_storage import DatedDfStorage
from storages.log_report import ParseLogReport

DIR_TICKERS_DATA = os.getcwd() + '/data/tickers'  # папка по умолчанию для списка тикеров
DIR_HISTORY_DATA = os.getcwd() + '/data/history'  # папка по умолчанию для исторических данных
FILE_SECTORS_DATA = os.getcwd() + '/data/sectors_estimize.txt'  # файл по умолчанию со списком секторов
LOG_REPORT_FL_NM = os.getcwd() + '/data/report.csv'  # default report file name
TIME_WIN_INITIAL = 3  # in days

def create_parser():
    parcer = argparse.ArgumentParser(
        prog='gathering_estimize',
        description='Scraping estimize.com: stock list tickers, announcement date, EPS and revenue, forecast and real'
    )
    subparcers = parcer.add_subparsers(
        dest='command',
        title='Possible commands',
        )

    gather_tic_parcer = subparcers.add_parser('gather_tickers')
    gather_tic_parcer.add_argument('--dir', default=DIR_TICKERS_DATA,
                                   help='Directory for tickers file, optional, default directory: {dir}'.format(dir=DIR_TICKERS_DATA))
    gather_tic_parcer.add_argument('--name', default=FILE_SECTORS_DATA,
                                   help='File name source sectors list, optional, default filename: {nm}'.format(nm=FILE_SECTORS_DATA))
    gather_tic_parcer.add_argument('--report_nm', default=LOG_REPORT_FL_NM,
                                   help='Report file name , optional, default filename: {nm}'.format(nm=LOG_REPORT_FL_NM))

    initial_gather_parcer = subparcers.add_parser('initial_pass')
    initial_gather_parcer.add_argument('--dir_tic', default=DIR_TICKERS_DATA,
                                       help='Directory for tickers file, optional, default directory: {dir}'.format(dir=DIR_TICKERS_DATA))
    initial_gather_parcer.add_argument('--dir_data', default=DIR_HISTORY_DATA,
                                       help='Directory for data file, optional, default directory: {dir}'.format(dir=DIR_HISTORY_DATA))
    initial_gather_parcer.add_argument('--date', default=datetime.date.today().strftime('%Y-%m-%d'),
                                       help='Tickers list date produced, optional, default is today date')
    initial_gather_parcer.add_argument('--report_nm', default=LOG_REPORT_FL_NM,
                                   help='Report file name , optional, default filename: {nm}'.format(nm=LOG_REPORT_FL_NM))
    initial_gather_parcer.add_argument('--time_window', default=TIME_WIN_INITIAL,
                                   help='Time window for initial gathering , optional, default {tw} days'.format(tw=TIME_WIN_INITIAL))


    return parcer


def gather_tickers(names, base_url):
    logger.info("Gather_tickers started")
    log_report = ParseLogReport(report_name=names.report_nm)
    if not os.path.exists(names.name):
        logger.error("Sector names file {f_n} doesn't exist".format(f_n=names.name))
        raise StopIteration
    try:
        df = get_tickers_df(names.name, base_url)
        tickers_storage = DatedDfStorage(date=datetime.date.today(),
                                         suffix='tickers',
                                         dir_name=names.dir)
        tickers_storage.write_dated_df(df)
        tic_dict = {tic: ['tic_load', 'OK', ''] for tic in df.index.tolist()}
        log_report.write_report(log_tic_dict=tic_dict)
        tic_check = log_report.check_report(tic_list=df.index.tolist(), func_nm='tic_load', start_dt=datetime.date.today())
        ok_count = sum(v == True for v in tic_check.values())
        err_count = sum(v == False for v in tic_check.values())
        logger.info("Gather_tickers ended, {n} tickers loaded, {n_err} errors occurred".format(n=ok_count, n_err=err_count))
    except Exception as e:
        tic_dict = {'': ['tic_load', 'ERROR', str(e)]}
        log_report.write_report(log_tic_dict=tic_dict)
        logger.error(e)


def initial_gather_data(names, base_url):
    start = time.time()
    logger.info("Initial gathering EPS&revenue data started")
    date_tickers_parsing = datetime.datetime.strptime(names.date, '%Y-%m-%d')
    tickers_storage = DatedDfStorage(date=date_tickers_parsing,
                                     suffix='tickers',  # суфикс для тестового файла
                                     dir_name=names.dir_tic)
    tickers_df = tickers_storage.read_dated_df(index_col='tic')
    data_storage = DatedDfStorage(date=pd.Timestamp(date_tickers_parsing).date(),
                                suffix='history_data',
                                dir_name=names.dir_data)
    _storage = DatedDfStorage(date=pd.Timestamp(date_tickers_parsing).date(),  # вспомогательный файл
                                suffix='_df',
                                dir_name=names.dir_data)
    log_report = ParseLogReport(report_name=names.report_nm)
    tic_check_dict = log_report.check_report(tic_list=tickers_df.index.tolist(), func_nm='initial_load',
                                        start_dt=datetime.date.today() - datetime.timedelta(names.time_window))
    for tic_name, tic_row in tickers_df.iterrows():
            if not tic_check_dict[tic_name]:
                try:
                    _df = get_history_data(date_tickers_parsing, tic_row)
                    try:  # пробуем прочитать старые данные
                        _df_old = data_storage.read_dated_df(header=[0, 1], index_col=[0, 1])
                        _storage.write_dated_df(_df)
                        _df = _storage.read_dated_df(header=[0, 1], index_col=[0, 1])  # rite/read для приведение к единому формату данных для concat
                        df = pd.concat([_df_old, _df], axis=1)
                    except FileNotFoundError:  # если файла нет - обрабатываем ошибку - пишем 1-ый тикер
                        df = _df
                    data_storage.write_dated_df(df)
                    tic_dict = {tic_name: ['initial_load', 'OK', '']}
                    log_report.write_report(log_tic_dict=tic_dict)
                    logger.info('Parsed {tic_nm} data. Elapsed time: {t:.1f} sec'.format(tic_nm=tic_name, t=time.time()-start))
                except Exception as e:
                    tic_dict = {tic_name: ['initial_report', 'ERROR', str(e)]}
                    log_report.write_report(log_tic_dict=tic_dict)
                    logger.error('Parsing the ticker {tic_nm} caused error: {err_str}'.format(tic_nm=tic_name, err_str=str(e)))
            else:
                pass  # ничего не делаем, если данные уже записаны и дата не протухла
    logger.info("Gathering historical EPS&revenue data ended")


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s:%(name)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.propagate = False
    parser = create_parser()
    namespace = parser.parse_args(sys.argv[1:])
    if namespace.command == 'gather_tickers':
        url = 'https://www.estimize.com/sectors/{sector}?per={max_tickers}'
        # f_sector_list = './data/sector_test.txt'
        gather_tickers(namespace, base_url=url)
    elif namespace.command == 'initial_pass':
        url = 'https://www.estimize.com/{ticker}/{quarter}?metric_name={metric}&chart=table'
        initial_gather_data(namespace, base_url=url)
    else:
        parser.print_help()