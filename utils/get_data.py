import pandas as pd
import pickle
import happybase
import struct
import numpy as np
import sys
# from influxdb import DataFrameClient
import re
from sqlalchemy.engine import create_engine
import time
from datetime import datetime, timezone, timedelta
# from utils.conf import sql_db_configs


def get_csv_data(path, header=None):
    """load padas dataframe from csv file

    Arguments:
        path {str} -- filepath of the csv file

    Returns:
        pandas.DataFrame -- loaded data
    """
    return pd.read_csv(path, sep=',', encoding='utf-8', header=header)


def get_pickle_data(path):
    """load data from pickle file

    Arguments:
        path {str} -- filepath of the pickle file

    Returns:
        object -- loaded data
    """
    with open(path, 'rb') as file:
        return pickle.load(file)


def get_df_from_hbase(con, table_name, key, cf='hb'):
    """Read a pandas DataFrame object from HBase table.

    Arguments:
        con {happybase.Connection} -- HBase connection object
        table_name {str} -- HBase table name to read
        key {str} -- row key from which the DataFrame should be read

    Keyword Arguments:
        cf {str} -- Column Family name (default: {'hb'})

    Returns:
        [pandas.DataFrame] -- Pandas DataFrame object read from HBase
    """

    table = con.table(table_name)

    column_dtype_key = key + 'columns'
    column_dtype = table.row(column_dtype_key, columns=[cf])
    columns = {col.decode().split(':')[1]: value.decode() for col, value in column_dtype.items()}

    column_order_key = key + 'column_order'
    column_order_dict = table.row(column_order_key, columns=[cf])
    column_order = list()
    for i in range(len(column_order_dict)):
        column_order.append(column_order_dict[bytes(':'.join((cf, str(i))), encoding='utf-8')].decode())

    # # row_start = key + 'rows' + struct.pack('>q', 0)
    # row_start = key + 'rows' + str(column_order(0))
    # # row_end = key + 'rows' + struct.pack('>q', sys.maxint)
    # row_end = key + 'rows' + str(column_order[len(column_order) - 1])
    row_key_template = key + '_rows_'
    # scan_columns = ['{}{}'.format(row_key_template, item) for item in column_order]
    HBase_rows = table.scan(row_prefix=bytes(row_key_template, encoding='utf-8'))
    # HBase_rows = table.scan(columns='cf:')
    df = pd.DataFrame()
    for row in HBase_rows:
        column_name = row[0].decode().split('_')[len(row[0].decode().split('_')) - 1]
        df_column = {key.decode().split(':')[1]: value.decode() for key, value in row[1].items()}
        pd_series = pd.Series(df_column)
        # df = df.append(df_column, ignore_index=True)
        df[column_name] = pd_series

    for column, data_type in columns.items():
        if len(list(columns.items())) == 1:
            column = df.columns[0]
        if column == '':
            continue
        try:
            df[column] = pd.to_numeric(df[column])
        except ValueError:
            pass
        df[column] = df[column].astype(np.dtype(data_type))
    return df


def get_data_from_cell(con, table_name, row_key, cf='hb'):
    table = con.table(table_name)

    cell = table.row(row_key, columns=[cf])
    type_set = set()
    columnsOrder = None
    SeriesName = None
    columnsType = None
    columnsOrder_cf = None
    SeriesName_cf = None
    columnsType_cf = None
    res = None
    for cf, value in cell.items():
        cf_qualifier = cf.decode().split(':')[1]
        data_type = cf_qualifier.split('_')[0]
        type_set.add(data_type)
        data_content = cf_qualifier.split('_')[1]
        if data_content == 'columnsOrder':
            columnsOrder = eval(value.decode())
            columnsOrder_cf = cf
        if data_content == 'SeriesName':
            SeriesName = value.decode()
            SeriesName_cf = cf
        if data_content == 'columnsType':
            try:
                columnsType = eval(value.decode())
            except NameError:
                columnsType = value.decode()
            columnsType_cf = cf

    if columnsOrder_cf is not None:
        cell.pop(columnsOrder_cf)
    if SeriesName_cf is not None:
        cell.pop(SeriesName_cf)
    if columnsType_cf is not None:
        cell.pop(columnsType_cf)
    cell_keys = cell.keys()

    if columnsOrder_cf in cell_keys or SeriesName_cf in cell_keys or columnsType_cf in cell_keys:
        raise ValueError('more than one clean_log input one cell')
    if len(type_set) > 2:
        raise ValueError('more than one clean_log input one cell')
    if 'DataFrame' in type_set:
        res = pd.DataFrame()
        for cf, value in cell.items():
            cf_qualifier = cf.decode().split(':')[1]
            data_index = cf_qualifier.split('_')[1]
            value = eval(value.decode())
            if 'None' in value:
                value = [None if v == 'None' else v for v in value]

            df_sub = pd.DataFrame(np.array(value).reshape(1, -1),
                                  columns=columnsOrder, index=[data_index])
            res = res.append(df_sub)
        for column, data_type in columnsType.items():
            if column == '':
                continue
            try:
                res[str(column)] = pd.to_numeric(res[str(column)])
            except ValueError:
                pass
            res[str(column)] = res[str(column)].astype(np.dtype(data_type))
    if 'Series' in type_set:
        res = pd.Series()
        for cf, value in cell.items():
            cf_qualifier = cf.decode().split(':')[1]
            data_index = cf_qualifier.split('_')[1]
            df_sub = pd.Series(value.decode(), index=[data_index])

            res = res.append(df_sub)
        if SeriesName is not None:
            res.name = SeriesName
        try:
            res = pd.to_numeric(res)
        except ValueError:
            pass
        res = res.astype(np.dtype(columnsType))
    if 'Others' in type_set:
        res = dict()
        for cf, value in cell.items():
            cf_qualifier = cf.decode().split(':')[1]
            data_key = cf_qualifier.split('_')[1]
            # value = value.decode()
            value = value
            try:
                value = value.decode()
                value = eval(value)
            except:
                pass
            res[data_key] = value
    return res


# def _get_data_from_influxdb(db_name, db_table, sql_cmd, port=18097):
#     from requests.exceptions import ChunkedEncodingError
#     from influxdb.exceptions import InfluxDBClientError
#     from json.decoder import JSONDecodeError
#     from influxdb.exceptions import InfluxDBServerError
#     host = 'db.cnecloud.cn'
#     port = '{}'.format(port)
#     user = ''
#     password = ''
#     # protocol='json'
#     client = DataFrameClient(host, port, user, password, db_name)
#     # atmost retry 3 times
#     for i in range(4):
#         try:
#             dic_res = client.query(sql_cmd)
#         except (ChunkedEncodingError, InfluxDBClientError, JSONDecodeError, InfluxDBServerError) as e:
#             if i < 3:
#                 print('Exception: {}, try again'.format(e))
#                 time.sleep(3)
#                 if isinstance(e, InfluxDBServerError):
#                     time.sleep(120)
#             else:
#                 print('Achieve Maximun Retrying Times, reraise error')
#                 raise
#         else:
#             break
#     try:
#         data_frame_res = dic_res[db_table]
#     except KeyError:
#         return None
#     return data_frame_res
#
#
# def get_data_from_influxdb(db_name, db_table, sql_cmd, start_time=None, end_time=None, port=18097):
#     """get data from inluxdb. auto select port by time range
#
#     Arguments:
#         db_name {str} -- database name
#         db_table {str} -- table name (measure name)
#         sql_cmd {str} -- sql command. "@start_time" and "@end_time" will be replaced by start_time and end_time respectively
#
#     Keyword Arguments:
#         start_time {datetime.datetime} -- query start time. Used to determine the database port (default: {None})
#         end_time {datetime.datetime} -- query end time.  Used to determine the database port(default: {None})
#         port {int} -- database port, if neither start_time or end_time is set, will use this port
# forcibly (default: {18097})
#
#     Returns:
#         pandas.DataFrame -- query data result
#     """
#     if start_time == None and end_time == None:
#         return _get_data_from_influxdb(db_name, db_table, sql_cmd, port)
#     if isinstance(start_time, str):
#         start_time = datetime.strptime(start_time, '%Y-%m-%d').replace(tzinfo=timezone(timedelta(hours=8)))
#     if isinstance(end_time, str):
#         end_time = datetime.strptime(end_time, '%Y-%m-%d').replace(tzinfo=timezone(timedelta(hours=8)))
#
#     dt_sp_time = datetime.strptime('2017-12-20', '%Y-%m-%d').replace(tzinfo=timezone(timedelta(hours=8)))
#     port = None
#     if start_time is not None and start_time >= dt_sp_time:
#         port = 18096
#     if end_time is not None and end_time <= dt_sp_time:
#         port = 18097
#     sql_cmd = sql_cmd.replace("@start_time", start_time.strftime("%Y-%m-%d %H:%M:%S"))
#     sql_cmd = sql_cmd.replace("@end_time", end_time.strftime("%Y-%m-%d %H:%M:%S"))
#     if port is not None:
#         return _get_data_from_influxdb(db_name, db_table, sql_cmd, port=port)
#
#     data1 = _get_data_from_influxdb(db_name, db_table, sql_cmd, port=18097)
#     data2 = _get_data_from_influxdb(db_name, db_table, sql_cmd, port=18096)
#     if data1 is None:
#         return data2
#     elif data2 is None:
#         return data1
#     return pd.concat([data1, data2])


# sql_db_configs = {
#     "PowerP_His": {
#         "db_user": 'dbreader@cnedb',
#         "db_pass": '0182@CNE',
#         "db_host": 'cnedb.database.chinacloudapi.cn',
#         "db_port": 1433,
#         "db_name": 'PowerP_His'
#     },
#     "PowerP_BI": {
#         "db_user": 'dbreader@cnedb',
#         "db_pass": '0182@CNE',
#         "db_host": 'cnedb.database.chinacloudapi.cn',
#         "db_port": 1433,
#         "db_name": 'PowerP_BI'
#     },
#     "PowerP_Base": {
#         "db_user": 'dbreader@cnedb',
#         "db_pass": '0182@CNE',
#         "db_host": 'cnedb.database.chinacloudapi.cn',
#         "db_port": 1433,
#         "db_name": 'PowerP_Base'
#     },
#     "ALGORITHM": {
#         "db_user": 'algoRoot@ffowgrtwpa',
#         "db_pass": 'algoAdmin123',
#         "db_host": 'ffowgrtwpa.database.chinacloudapi.cn',
#         "db_name": 'ALGORITHM'
#     }
# }


def get_sql_engine(db_user, db_pass, db_host, db_name, db_port=1433):
    connect_info = "mssql+pymssql://{}:{}@{}:{}/{}?charset=utf8".format(db_user, db_pass, db_host, db_port, db_name)
    engine = create_engine(connect_info)
    return engine


def get_data_from_sql(sql_cmd, db_config='PowerP_His'):
    if type(db_config) is str:
        db_config = sql_db_configs[db_config]
    engine = get_sql_engine(**db_config)
    df = pd.read_sql(sql=sql_cmd, con=engine)
    return df


def get_device_code(station_code, device_type):
    engine = get_sql_engine(**sql_db_configs['PowerP_His'])
    sql_cmd = 'SELECT DISTINCT DeviceCode FROM  V_Device  WHERE  StationCode ={}'.format(station_code)
    df = pd.read_sql(sql=sql_cmd, con=engine)['DeviceCode']
    # pattern = re.compile(r'M110M')
    # match = pattern.match(df)
    # res = [re.findall('(^[0-9]+M101M.*)', item) for item in df]
    res = []
    for item in df:
        m = re.match('(^[0-9]+M{}M.*)'.format(device_type), item)
        if m is not None:
            n = m.group()
            res.append(n)

    return res


def get_ALOGRITHM_sql_engine():
    engine = get_sql_engine(**sql_db_configs['ALGORITHM'])
    return engine


def get_station_name(station_code):
    """
    :param station_code: 
    :type int
    :return: station name
    """
    engine = get_sql_engine(**sql_db_configs['PowerP_His'])
    sql_cmd = "SELECT StationName from Station WHERE  StationCode ={}".format(station_code)
    station_name = pd.read_sql(sql=sql_cmd, con=engine)['StationName'][0]
    return station_name


