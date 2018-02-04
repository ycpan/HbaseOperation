import numpy as np
import happybase
import pandas as pd


def create_HBase_table(con, table_name, table_cf: dict):
    con.create_table(table_name, families=table_cf)


def save_data_to_cell(data_tuple_list, con, table_name, row_key, cf):
    table_names = con.tables()
    if bytes(table_name, encoding='utf-8') not in table_names:
        raise NameError("the table {} have not in this database".format(table_name))
    table = con.table(table_name)

    for desc, data in data_tuple_list:

        if isinstance(data, pd.DataFrame):
            dtype_column_qualifier = 'DataFrame_columnsType'
            order_column_qualifier = 'DataFrame_columnsOrder'
            dtype_column_value = dict()
            data_qualifier_prefix = "row_"
            type_dic = {column: data.dtypes[column].name for column in data.columns}

            dtype_column_value[':'.join((cf, str(dtype_column_qualifier)))] = str(type_dic)
            order_list = [str(column) for column in data.columns]
            order_column_value = dict()
            order_column_value[':'.join((cf, str(order_column_qualifier)))] = str(order_list)

            with table.batch(transaction=True) as b:
                b.put(row_key, dtype_column_value)
                b.put(row_key, order_column_value)
                data = data.fillna('None')
                for index, value in data.iterrows():
                    row_value = dict()
                    data_qualifier = data_qualifier_prefix + index
                    row_value[':'.join((cf, str(data_qualifier)))] = str(value.tolist())
                    b.put(row_key, row_value)
        elif isinstance(data, pd.Series):
            dtype_column_qualifier = 'Series_columnsType'
            series_name_qualifier = 'Series_SeriesName'
            data_qualifier_prefix = "row_"
            dtype_column_value = dict()
            series_name_value = dict()
            dtype_column_value[':'.join((cf, dtype_column_qualifier))] = data.dtypes.name
            series_name_value[':'.join((cf, series_name_qualifier))] = data.name
            with table.batch(transaction=True) as b:
                b.put(row_key, dtype_column_value)
                b.put(row_key, series_name_value)
                row_value = dict()
                for index, value in data.items():
                    data_qualifier = data_qualifier_prefix + index
                    row_value[':'.join((cf, str(data_qualifier)))] = str(value.tolist())
                    b.put(row_key, row_value)
                b.put(row_key, row_value)
        else:
            data_qualifier = 'Others_' + desc
            value = {':'.join((cf, data_qualifier)): str(data)}
            with table.batch(transaction=True) as b:
                b.put(row_key, value)


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
                res[column] = pd.to_numeric(res[column])
            except ValueError:
                pass
            res[column] = res[column].astype(np.dtype(data_type))
    if 'Series' in type_set:
        res = pd.Series()
        for cf, value in cell.items():
            cf_qualifier = cf.decode().split(':')[1]
            data_index = cf_qualifier.split('_')[1]
            df_sub = df_sub = pd.Series(value.decode(), index=[data_index])

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
            value = value.decode()
            try:
                value = eval(value)
            except NameError:
                pass
            res[data_key] = value
    return res

