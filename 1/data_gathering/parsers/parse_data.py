# -*- coding: utf-8 -*-
import logging
import pandas as pd
from bs4 import BeautifulSoup
import time

from datetime import datetime
from datetime import date
from storages.dated_df_storage import DatedDfStorage

from scrappers.scrapper import Scrapper

URL = 'https://www.estimize.com/{ticker}/{quarter}?metric_name={metric_name}&chart=table'
METRICS = ['eps', 'revenue']
LOCATOR_PATHS = {'metric_table': 'rel-chart-tbl', 'sector': 'release-header-information-breadcrumb'}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_url(base_url, field, ticker, quarter_str):
    '''
    :param base_url: https://www.estimize.com/{ticker}/{quarter}?metric_name={metric_name}&chart=table
    '''
    return base_url.format(metric_name=field, ticker=ticker, quarter=quarter_str)


def get_quarter(date):
    '''
    Returns a string of the form Year + Quarter, given a date
    Ex. Given a string of "2014-01-01",
    this method will return 2014Q1
    '''
    date = pd.Timestamp(date)
    return str(date.year) + 'Q' + str(date.quarter)


def ez2qurter(ez_q_str):
    # формат estimize FQq ‘yy -> формат квартала yyyyQq
    q_str, y_str = ez_q_str[1:].split(' ‘')
    return '20' + y_str + q_str


def ez2qurter_slen(ez_q_str):
    # формат estimize FQq ‘yy -> формат квартала yyyyQq
    q_str, y_str = ez_q_str[1:].split(" '")
    return '20' + y_str + q_str

def quarter2ez(q_str):
    # формат квартала yyyyQq -> формат url estimize fqQ-YYYY
    ez_y_str, ez_q_str = q_str.split('Q')
    return 'fq' + ez_q_str + '-' + ez_y_str


def get_quarter_page_list(q_start):
    # create list if quarter dates to parsing pages
    y_strt, q_strt = [int(s) for s in q_start.split('Q')]
    q_str_lst = []
    for n_q in range(6, 70, 12):  # начальный откат на 6 кварталов, потом по 3 года - 12 кварталов
        y = y_strt - n_q // 4
        q = q_strt - n_q % 4
        q_str_lst.append(str(y) + 'Q' + str(q))
    return q_str_lst


def mark_sources(s_list):
    # mark sources for indexing use
    mhl_list = ['Mean', 'High', 'Low']
    base_str = s_list[0]
    list_out = [s_list[0]]
    for s in s_list[1:]:
        if s in mhl_list:
            list_out.append(base_str[0] + '_' + s.lower())
        else:
            base_str = s
            list_out.append(s)
    return list_out


def parse_table_txt(t_txt):
    t_txt = t_txt.replace('Wall St.', 'Wall_St.')
    # t_txt = t_txt.replace(',', '.')  # for string to float converting - запятые используются для тысяч
    # t_txt = t_txt.replace('-', 'NaN')  # for string to float converting - "-" используюется как минус : TODO
    line_txt_lst = [l for l in iter(t_txt.splitlines())]
    mertic_str = line_txt_lst[0].split(' ')[0]
    columns_nm = ['sources'] + [ez2qurter_slen('F' + s) for  s in line_txt_lst[0].split(' F')[1:]]  # 1-st line without 1-st item
    df = pd.DataFrame([l.split(' ') for l in line_txt_lst[2:-1]], columns=columns_nm)  # without 'You' and 'YoY Growth' lines
    df['sources'] = mark_sources(df['sources'])  # mark sources for indexing use
    df.set_index('sources', inplace=True)
    df = df.apply(lambda x: pd.to_numeric(x.astype(str).str.replace(',',''), errors='coerce'))
    return (mertic_str, df)


def compute_q_shift(current_dt, company_qrt):
    # compute date shift in quarter company_qrt - calnder_qrt
    # calendar_qrt = previous quarter from current date
    return (pd.Timestamp(company_qrt).year * 4 + pd.Timestamp(company_qrt).quarter) - (
            pd.Timestamp(current_dt).year * 4 + pd.Timestamp(current_dt).quarter - 1)  # previous quarter from current date


def transform_df_to_concat(tic_str, mertic_str, q_shift, df):
    company_qrt_col_nm = tic_str + '_qrt'
    s = df.unstack()  # transform to series
    df_out = s.reset_index()
    df_out.rename({'level_0':company_qrt_col_nm, 0:tic_str}, axis='columns', inplace=True)
    df_out['metric'] = mertic_str
    df_out[company_qrt_col_nm] = pd.to_datetime(df_out[company_qrt_col_nm]).dt.to_period('Q')
    df_out['calendar_qrt'] = df_out[company_qrt_col_nm] - q_shift

    return df_out.set_index(['metric', 'calendar_qrt', 'sources'])


def parse_ticker_data(tic_str, q_shift, date_str_lst, metric_str_lst, base_url):
    # create urls list, parse data, convert to Pandas series, concat in serie for  ticker
    urls = [base_url.format(ticker=tic_str, quarter=quarter2ez(q_str), metric_name=met_str) for q_str in date_str_lst
                                                                                            for met_str in metric_str_lst]
    scraper = Scrapper()
    # скачаем таблицы данных в текстовом формате
    # <table class="rel-chart-tbl">
    # class ="release-header-information-breadcrumb"
    table_txt_list = scraper.scrap_slenium(urls, LOCATOR_PATHS)
    s_lst = []
    for tb_txt in table_txt_list:
        if tb_txt['metric_table']is not None:
            mtrc, df = parse_table_txt(tb_txt['metric_table'])
            s_lst.append(transform_df_to_concat(tic_str=tic_str, mertic_str=mtrc, q_shift=q_shift, df=df))  # get series list
    # нужно добавить индекс календарного квартала : TODO
    return pd.concat(s_lst).sort_index(level='calendar_qrt')


def get_history_data(current_date, tics_df):
    tics_df.set_index('tic', inplace=True)
    tic_df_list = []
    for index, row in tics_df.iterrows():
        ticker = index
        qrt_start = ez2qurter(row['Qurter'])
        q_shift = compute_q_shift(current_dt=current_date, company_qrt=qrt_start)
        date_str_lst = get_quarter_page_list(qrt_start)
        tic_df_list.append(parse_ticker_data(tic_str=ticker,
                                             q_shift=q_shift,
                                             date_str_lst=date_str_lst,
                                             metric_str_lst=METRICS,
                                             base_url=URL))
    return pd.concat(tic_df_list, axis=1)





if __name__ == '__main__':
    dt_str = '2019-02-05'
    start = time.time()
    date_tickers_parsing = datetime.strptime(dt_str, '%Y-%m-%d')
    tickers_storage = DatedDfStorage(date=date_tickers_parsing,
                                     suffix='tickers_test',  # суфикс для тестового файла
                                     dir_name='C:\\Users\\ersan\\PycharmProjects\\DS_OTUS\\1\\data_gathering\\data\\tickers')
    tickers_df = tickers_storage.read_dated_df()
    df = get_history_data(dt_str, tickers_df)

    print('Completed {n} quarters for {m} tickers download in {t} seconds'.format(n=df.shape[0] / 16,
                                                                                  m=df.shape[1] / 2,
                                                                                  t=time.time() - start))

    tickers_storage = DatedDfStorage(date=date.today(),
                                     suffix='history_data',
                                     dir_name='C:\\Users\\ersan\\PycharmProjects\\DS_OTUS\\1\\data_gathering\\data\\tickers')
    tickers_storage.write_dated_df(df)




    # tick = 'GM'
    # qrt_start = '2018Q4'
    # date_str_lst = get_quarter_page_list(qrt_start)
    # metricks = ['eps', 'revenue']
    # tick_serie = parse_ticker_data(tic_str=tick, date_str_lst=date_str_lst, metric_str_lst=metricks, base_url=url)
    # print(tick_serie)
