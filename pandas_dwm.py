"""
.. py:module:: pandas_dwm
    :platform: Unix
.. note::
    Pandas Data Wrapper Manipulation functions.
.. moduleauthor:: `Bernardo Costa <bernardoantunescosta at gmail.com>`
"""

import pandas as pd
import awswrangler as wr


def create_df(cols, rows):
    """ 
        Return a Pandas DataFrame based on cols and rows. 
        
        Parameters: 
            cols (List of strings): List of columns name.
            rows (List of Lists): List of rows (each list is a row) 
          
        Returns: 
            pandas.DataFrame: with columns cols and Rows rows. 
    """
    return pd.DataFrame(rows, columns=cols) 


def cast_column(df, col, _type):
    """ 
        Return a DataFrame Column casted with '_type'. 
        
        Parameters: 
            df (pandas.DataFrame): The DataFrame.
            col (string): column to cast.
            _type (Type): Type to cast (ex: str, int, float, ...).
          
        Returns: 
            pandas.DataFrame: with columns cols and Rows rows. 
    """
    return df[col].astype(_type)


def read_athena(boto3_session, query, db, params=None, ctas_approach=False):
    """ 
        Return pandas.DataFrame running a Query in Athena.
        
        Parameters: 
            session (boto3.session.Session): boto3 custom Session.
            query (string): Athena query with python format string.
            db (string): Athena DataBase name.
            params (Dict): Dict with param name and param value.
          
        Returns: 
            pandas.DataFrame: Pandas DataFrame based on athena Query. 
    """
    if params is None:
        df = wr.athena.read_sql_query(
            sql=query,
            database=db,
            boto3_session=boto3_session,
            ctas_approach=ctas_approach)
    else:
        df = wr.athena.read_sql_query(
            sql=query.format(**params),
            database=db,
            boto3_session=boto3_session,
            ctas_approach=ctas_approach)
    return df


def join(df1, df2, left_on=None, right_on=None, on=None, how='inner', _suffixes=('', '_y')):
    """ 
        Return 'df1' joined 'df2' based on given conditions. Use 'on' or 'left_on' and 'right_on'.
        
        Parameters: 
            df1 (pandas.DataFrame): Left DataFrame.
            df2 (pandas.DataFrame): Right DataFrame.
            left_on (List of Strings): Left Columns to join.
            right_on (List of Strings): Right Columns to join.
            on (List of Strings): Columns to join.
            how (string): How to join ('left', 'right', 'outer', 'inner'}
            _suffixes (tuple): tuple (left_suffix, right_suffix). In case to duplicated columns.
          
        Returns: 
            pandas.DataFrame: 'df1' joined 'df2' based on given conditions.
    """
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
    elif how == 'mean':
        df = df.groupby(columns)[on].mean().reset_index()
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


def send_parquet_to_s3(boto3_session, df, database, s3_path, partition_cols=['p_ano', 'p_mes', 'p_dia'],mode='append', dataset=True):
     return wr.s3.to_parquet(
            df=df,
            index=False,
            database=database,
            path=s3_path,
            partition_cols=partition_cols,
            boto3_session=boto3_session,
            mode=mode,
            dataset=dataset)
        

def delete_from_athena(session, s3_resource, query, db, params=None):
    """ query format "SELECT DISTINCT("$path")" ..."""
    df = read_athena(session, query, db, params=None)
    _delete_s3_object(s3_resource, path)

        
def _drop_y(df):
    # list comprehension of the cols that end with '_y'
    to_drop = [x for x in df if x.endswith('_y')]
    return df.drop(to_drop, axis=1, inplace=True)


def _delete_s3_object(df, s3_conn, path):
    
    def bucket_and_key_from_path(path):
        parts = path[5:].split("/")
        bucket = parts.pop(0)
        key = "/".join(parts)
        return bucket, key

    def load_itens_to_delete():
        bucket_name = ''
        itens_to_delete = {'Objects': [], 'Quiet': True}
        for t in df.itertuples():
            bucket_name = t.path[0]
            itens_to_delete['Objects'].append({'Key': t.path[1]})
        return bucket_name, itens_to_delete
    
    df['path'] = apply_lambda(df, '$path', bucket_and_key_from_path)
    bucket_name, itens_to_delete = load_itens_to_delete()
    
        
    bucket = s3.Bucket(bucket_name)
    result = bucket.delete_objects(Delete=delete)
