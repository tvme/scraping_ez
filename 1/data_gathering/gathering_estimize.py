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


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DIR_TICKERS_DATA = os.getcwd() + '/data/tickers'  # папка по умолчанию для списка тикеров
DIR_HISTORY_DATA = os.getcwd() + '/data/history'  # папка по умолчанию для исторических данных
FILE_SECTORS_DATA = os.getcwd() + '/data/sectors_estimize.txt'  # файл по умолчанию со списком секторов


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

    initial_gather_parcer = subparcers.add_parser('initial_pass')
    initial_gather_parcer.add_argument('--dir_tic', default=DIR_TICKERS_DATA,
                                       help='Directory for tickers file, optional, default directory: {dir}'.format(dir=DIR_TICKERS_DATA))
    initial_gather_parcer.add_argument('--dir_dt', default=DIR_HISTORY_DATA,
                                       help='Directory for data file, optional, default directory: {dir}'.format(dir=DIR_HISTORY_DATA))
    initial_gather_parcer.add_argument('--date', default=datetime.date.today().strftime('%Y-%m-%d'),
                                       help='Tickers list date produced, optional, default is today date')

    return parcer


def gather_tickers(names, base_url):
    logger.info("Gather_tickers started")
    if not os.path.exists(names.name):
        logger.info("File {f_n} doesn't exist".format(f_n=names.name))
        raise StopIteration
    df = get_tickers_df(names.name, base_url)
    tickers_storage = DatedDfStorage(date=datetime.date.today(),
                                     suffix='tickers',
                                     dir_name=names.dir)
    tickers_storage.write_dated_df(df)
    logger.info("Gather_tickers ended")


def initial_gather_data(names, base_url):
    start = time.time()
    logger.info("Initial gathering EPS&revenue data started")
    date_tickers_parsing = datetime.datetime.strptime(names.date, '%Y-%m-%d')
    tickers_storage = DatedDfStorage(date=date_tickers_parsing,
                                     suffix='tickers_test',  # суфикс для тестового файла
                                     dir_name=names.dir_tic)
    tickers_df = tickers_storage.read_dated_df()
    tickers_df.set_index('tic', inplace=True)  
    df = get_history_data(date_tickers_parsing, tickers_df)
    print('Completed {n} quarters for {m} tickers download in {t} seconds'.format(n=df.shape[0] / 8,  # 8 полей из таблицы
                                                                                  m=df.shape[1] / 3,  # 3 colunms for ticker
                                                                                  t=time.time() - start))

    dt_storage = DatedDfStorage(date=pd.Timestamp(date_tickers_parsing).date(),
                                     suffix='history_data',
                                     dir_name=names.dir_dt)
    dt_storage.write_dated_df(df)
    logger.info("Gathering historical EPS&revenue ended")


if __name__ == '__main__':
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