# -*- coding: utf-8 -*-
import logging
import os
import datetime
import pandas as pd

from storages.dated_storage import DatedStorage

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
        logger.debug('Save data table in {f_nm}'.format(f_nm=f_name))
    else:  # save file in castom dir
        os.makedirs(dir_name)
        logger.debug('Create folder and save data table in {f_nm}'.format(f_nm=f_name))
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

    def read_dated_df(self, **kwargs):
        f_name = '{dir_path}/{date_str}_{suffix}.csv'.format(dir_path=self.dir_name,
                                                             date_str=self.date.strftime('%Y-%m-%d'),
                                                             suffix=self.suffix)
        if not os.path.exists(f_name):
            logger.debug("File {f_n} dosn't exist".format(f_n=f_name))
            raise FileNotFoundError

        with open(f_name) as f:
            logger.debug('Read {f_n}'.format(f_n=f_name))
            return pd.read_csv(f_name, **kwargs)


    def write_dated_df(self, df):
        """
        :param df: Pandas DataFrame
        should be written as table in  csv-file
        """
        date_str = self.date.strftime('%Y-%m-%d')
        write_ddf(df, date_str, self.suffix, self.dir_name)



if __name__ == '__main__':
    # test for dated DF storage
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s:%(name)s: %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.propagate = False
    df_test = pd.DataFrame({'a': [1, 2, 3]})
    date_test = datetime.date(2018, 12, 30)
    dir_test = './data'
    storage = DatedDfStorage(date_test, 'test', dir_test)
    storage.write_dated_df(df_test)
    print(storage.read_dated_df())
