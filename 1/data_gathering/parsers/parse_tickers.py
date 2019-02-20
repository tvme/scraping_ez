# -*- coding: utf-8 -*-
import logging
import pandas as pd
from bs4 import BeautifulSoup
import time
from scrappers.scrapper import Scrapper


MAX_TICKERS = '999' # максимальное количество тикеров на странице сектора

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_sectors_txt(f_name):
    sector_lst = []
    with open(f_name, 'r',  encoding='utf-8') as sectors_file:
        for line in sectors_file:
            line = line.lower()
            l_list = line.rstrip().split(' ')
            sector_name = '-'.join(l_list)
            sector_lst.append(sector_name)
    return sector_lst


def parse_tickers(sect, base_url):
    # url = 'https://www.estimize.com/sectors/{sector}?per={max_tickers}'.format(sector=sect, max_tickers=MAX_TICKERS)
    url = base_url.format(sector=sect, max_tickers=MAX_TICKERS)
    scraper = Scrapper()
    soup = BeautifulSoup(scraper.scrap_process(url), 'lxml')  # lxm parser
    tickers_table_dict = {}
    # опредилим год отсчета
    season_txt = soup.find('div', {'class': 'season'}).find('strong').text
    year_txt = season_txt.split(' ')[1]
    # скачаем таблицу тикеров
    tickers_html = soup.find('div', {'class': 'linked-table'})
    items = tickers_html.find_all('a', {'class': ['linked-row opened', 'linked-row closed']})
    for item in items:
        line_dict = {}
        ticker_nm = item.find('div', {'class': 'td symbol'}).text.replace('\n', '')
        if ticker_nm not in tickers_table_dict.keys():  # бывает, что в 1-м квартале 2 отчета, оставляем самый ранний
            date_str = item.find('div', {'class': 'td reports'}).text.replace('\n', '')[:-3] + ' ' + year_txt
            line_dict['Report'] = pd.to_datetime(date_str, format='%b %d %Y')
            line_dict['Time'] = item.find('div', {'class': 'td reports'}).text.replace('\n', '')[-3:]
            line_dict['Qurter'] = item.find('div', {'class': 'td quarter'}).text.replace('\n', '')
            line_dict['Sector'] = sect
            tickers_table_dict[ticker_nm] = line_dict
    df_out = pd.DataFrame(tickers_table_dict).T
    df_out.index.rename('tic', inplace=True)
    return df_out


def get_tickers_df(f_sectors_nm, tickers_url):
    sector_lst = parse_sectors_txt(f_sectors_nm)
    first_sector = True
    for sector in sector_lst:
        time.sleep(1)
        tickers_df = parse_tickers(sector, tickers_url)
        logger.info('Scrap {tic_num} tickers from {sect}'.format(tic_num=tickers_df.shape[0], sect=sector))
        if first_sector:
            all_tickers_df = tickers_df
            first_sector = False
        else:
            all_tickers_df = pd.concat([all_tickers_df, tickers_df])
    return all_tickers_df


if __name__ == '__main__':
    url = 'https://www.estimize.com/sectors/{sector}?per={max_tickers}'
    # f_name = './data/subsectors_estimize.txt'
    f_name = './data/sectors_estimize.txt'
    df = get_tickers_df(f_name, tickers_url=url)
    print('DataFrame shape = {}'.format(df.shape))
    print(df)
    index_duplicated = True in df.index.duplicated()
    print('Index_duplicated =', index_duplicated)