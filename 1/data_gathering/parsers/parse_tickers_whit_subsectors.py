# -*- coding: utf-8 -*-
import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
# from lxml import html

BIG_SECTOR_MARC = '»'

def parse_sub_sector_name(line_lst):
    sector_name_lst = []
    for wd in line_lst:
        if wd == '&': sector_name_lst.append('and')
        else: sector_name_lst.append(wd)
    sub_sector_name = '-'.join(sector_name_lst).lower()
    return sub_sector_name

def parse_sectors_txt(f_name):
    sectors_dict = {}
    sub_sector_lst = []
    glob_sector_name = 'first pass'
    with open(f_name, 'r',  encoding='utf-8') as sectors_file:
        for line in sectors_file:
            line = line.lower()
            line = line.replace(',', '')
            line = line.replace('.', ' dot')
            l_list = line.rstrip().split(' ')
            if BIG_SECTOR_MARC in l_list:
                if glob_sector_name != 'first pass':
                    sectors_dict[glob_sector_name] = sub_sector_lst
                sub_sector_lst = []
                glob_sector_name = '-'.join(l_list[:-1])
            else:
                sub_sector_lst.append(parse_sub_sector_name(l_list))
        sectors_dict[glob_sector_name] = sub_sector_lst
    return sectors_dict


def load_html_data(sector_url, session):
    request = session.get(sector_url)
    return request.text


def parse_tickers(sect, sub_sect):
    url = 'https://www.estimize.com/sectors/{sector}/industries/{sub_sector}?per=300'.format(sector=sect,
                                                                                             sub_sector=sub_sect)
    tickers_table_dict = {}
    soup = BeautifulSoup(load_html_data(url, s), 'lxml')  # , 'html.parser'
    # опредилим год отсчета
    season_txt = soup.find('div', {'class': 'season'}).find('strong').text
    year_txt = season_txt.split(' ')[1]
    # скачаем таблицу тикеров
    tickers_html = soup.find('div', {'class': 'linked-table'})
    items = tickers_html.find_all('a', {'class': 'linked-row opened'})
    for item in items:
        line_dict = {}
        ticker_nm = item.find('div', {'class': 'td symbol'}).text.replace('\n', '')
        if ticker_nm not in tickers_table_dict.keys():  # бывает, что в 1-м квартале 2 отчета, оставляем самый ранний
            date_str = item.find('div', {'class': 'td reports'}).text.replace('\n', '')[:-3] + ' ' + year_txt
            line_dict['Report'] = pd.to_datetime(date_str, format='%b %d %Y')
            line_dict['Time'] = item.find('div', {'class': 'td reports'}).text.replace('\n', '')[-3:]
            line_dict['Qurter'] = item.find('div', {'class': 'td quarter'}).text.replace('\n', '')
            line_dict['Sect.SubSect'] = '.'.join([sect, sub_sect])
            tickers_table_dict[ticker_nm] = line_dict
    return pd.DataFrame(tickers_table_dict).T

if __name__ == '__main__':
    # f_name = './data/sector_test.txt'
    # f_name = './data/subsectors_estimize.txt'
    f_name = './data/sectors_estimize.txt'
    dict = parse_sectors_txt(f_name)
    # establishing session
    s = requests.Session()
    s.headers.update({'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:45.0) Gecko/20100101 Firefox/45.0'})
    first_subSector = True
    for sector, sub_sector_list in dict.items():
        for sub_sector in sub_sector_list:
            time.sleep(1)
            tickers_df = parse_tickers(sector, sub_sector)
            print('Scrap {tic_num} tickers from {sect}.{sub_sect}'.format(tic_num = tickers_df.shape[0],
                                                                          sect = sector,
                                                                          sub_sect = sub_sector))
            if first_subSector:
                all_tickers_df = tickers_df
                first_subSector = False
            else:
                all_tickers_df = pd.concat([all_tickers_df, tickers_df])
    # print(all_tickers_df)
    print('DataFrame shape = {}'.format(all_tickers_df.shape))
    print(all_tickers_df)
    index_duplicated = True in all_tickers_df.index.duplicated()
    print('Index_duplicated =', index_duplicated)