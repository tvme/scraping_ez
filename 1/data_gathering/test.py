# -*- coding: utf-8 -*-
import pandas as pd
import datetime
from storages.dated_df_storage import DatedDfStorage


if __name__ == '__main__':
    # test for dated DF storage
    df_test = pd.DataFrame({'a': [1, 2, 3]})
    date_test = datetime.date(2018, 12, 30)
    dir_test = './data'

    storage = DatedDfStorage(date_test, 'test', dir_test)
    storage.write_dated_df(df_test)
    print(storage.read_dated_df())
