# -*- coding: utf-8 -*-
import logging
import os
import datetime
import pandas as pd

from storages.dated_storage import DatedStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write_ddf(df, date_str, suff, dir_name):
    '''
    запись датированноге DataFrame
    :param df: Pandas DataFrame
    :param date_str: data sa string
    :param suff:
    :param dir_name: data's dir
    :return: None
    '''
    f_name = dir_name + '/' + date_str + '_' + suff + '.csv'
    if os.path.exists(dir_name) or os.path.exists('./' + dir_name):
        logger.info('Save tickers table in {f_nm}'.format(f_nm=f_name))
    else:  # save file in castom dir
        os.makedirs(dir_name)
        logger.info('Create folder and save tickers table in {f_nm}'.format(f_nm=f_name))
    df.to_csv(f_name)


class DatedDfStorage(DatedStorage):
    '''
    for read/write DataFrame into dated file.
    Params class object: date, suffix, dir_name.
    DataFrame send into class object
    '''
    def __init__(self, date, suffix, dir_name):
        self.dir_name = dir_name
        self.date = date
        self.suffix = suffix

    def read_dated_df(self):
        f_name = '{dir_path}/{date_str}_{suffix}.csv'.format(dir_path=self.dir_name,
                                                             date_str=self.date.strftime('%Y-%m-%d'),
                                                             suffix=self.suffix)
        if not os.path.exists(f_name):
            logger.info("File {f_n} dosn't exist".format(f_n=f_name))
            raise StopIteration

        with open(f_name) as f:
            logger.info('Read {f_n}'.format(f_n=f_name))
            return pd.read_csv(f_name)


    def write_dated_df(self, df):
        """
        :param df: Pandas DataFrame
        should be written as table in  csv-file
        """
        date_str = self.date.strftime('%Y-%m-%d')
        write_ddf(df, date_str, self.suffix, self.dir_name)



if __name__ == '__main__':
    # test for dated DF storage
    df_test = pd.DataFrame({'a': [1, 2, 3]})
    date_test = datetime.date(2018, 12, 30)
    dir_test = './data'
    storage = DatedDfStorage(date_test, 'test', dir_test)
    storage.write_dated_df(df_test)
    print(storage.read_dated_df())
