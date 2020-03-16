import pandas as pd
import awswrangler


def create_df(cols, rows):
    return pd.DataFrame(rows, columns=cols) 


def cast_column(df, col, _type):
    return df[col].astype(_type)


def read_athena(session, query, db, params=None):
    """Return Pandas DataFrame running a Query in Athena."""
    
    if params is None:
        df = session.pandas.read_sql_athena(
            sql=query,
            database=db)
    else:
        df = session.pandas.read_sql_athena(
            sql=query.format(**params),
            database=db)
    return df


def join(df1, df2, left_on=None, right_on=None, on=None, how='left', _suffixes=('', '_y')):
    """Return d1 join d2 on based conditions."""
    
    if on is None:
        df = pd.merge(df1, df2,left_on=left_on, right_on=right_on, how=how, suffixes=_suffixes)
    elif right_on is None and left_on is None:
         df = pd.merge(df1, df2, on=on, how=how, suffixes=_suffixes)
    else:
        raise Exception('Erros on the selected parameters.')
    _drop_y(df)
    return df


def groupby(df, columns, on, how='sum'):
    """Return d1 grouped on based conditions."""
    if how == 'sum':
        df = df.groupby(columns)[on].sum().reset_index()
    else:
        raise Exception('Only sum implemented.')
    return df


def drop_duplicates(df):
    """Return df without duplicates."""
    return df.drop_duplicates()


def drop_columns(df, cols):
    return df.drop(columns=cols)

def filter_column(df, col, list_of_values):
    return df[df[col].isin(list_of_values)]


def get_columns_difference(df1, df2):
    return df1.columns.difference(df2.columns)


def select_columns(df, cols):
    return df[cols].copy()


def apply_lambda(df, col, func, empty=False):
    if empty:
        return df[col].apply(lambda _: func())
    else:
        return df[col].apply(lambda x: func(x))
    

def apply_df_lambda(df, cols, func):
    return df.apply(lambda row: func(*(row[cols])), axis=1)


def load_const_column(df, col, const):
    df[col] = const
    return df


def load_date_partition_cols(df, col, partition_cols_config={'year': 'p_ano', 'month': 'p_mes', 'day':'p_dia'}):
    for key, value in partition_cols_config.items():
        if key == 'year':
            df[value] = df[col].dt.year
        elif key == 'month':
            df[value] = df[col].dt.month
        elif key == 'day':
            df[value] = df[col].dt.day
        else:
            raise Exception("partition_cols_config' must be a dict with year, month, and day.")
    return df


def send_parquet_to_s3(session, df, database, s3_path, partition_cols=['p_ano', 'p_mes', 'p_dia']):
     return session.pandas.to_parquet(
            dataframe=df,
            preserve_index=False,
            database=database,
            path=s3_path,
            partition_cols=None)
        

def _drop_y(df):
    # list comprehension of the cols that end with '_y'
    to_drop = [x for x in df if x.endswith('_y')]
    return df.drop(to_drop, axis=1, inplace=True)
