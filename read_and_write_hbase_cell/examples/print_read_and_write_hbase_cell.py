import pandas as pd
import numpy as np
import happybase
from read_and_write_hbase_cell.source_code import create_HBase_table
from read_and_write_hbase_cell.source_code import save_data_to_cell
from read_and_write_hbase_cell.source_code import get_data_from_cell

def test_Series_cell():
    row_key = 'svc.test.Series'
    indexs = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'k']
    df = pd.DataFrame(np.random.randn(10, 5), columns=['a', 'b', '1', 'd', 'e'], index=indexs)
    df['c'] = 'hello world'
    df = df['a']
    df['f'] = None
    print("input:")
    print(df)
    column_content_list = [('test_Series', df)]
    save_data_to_cell(column_content_list, connection, table_name, row_key, 'test_Series')
    df = get_data_from_cell(connection, table_name, row_key, 'test_Series')
    print("output:")
    print(df)


def test_DataFrame_cell():
    row_key = 'svc.test.DataFrame'
    indexs = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'k']
    df = pd.DataFrame(np.random.randn(10, 5), columns=['a', 'b', '1', 'd', 'e'], index=indexs)
    df['c'] = 'hello world'
    df['a']['f'] = None
    print("input:")
    print(df)
    column_content_list = [('test_DataFrame', df)]
    save_data_to_cell(column_content_list, connection, table_name, row_key, 'test_DataFrame')
    df = get_data_from_cell(connection, table_name, row_key, 'test_DataFrame')
    print("output:")
    print(df)


def test_others_cell():
    row_key = 'svc.test.others'
    accuracy = 0.5
    precison = 0.8
    recall = 0.2
    f1 = 0.9
    algrithm = 'svc.svm'
    column_content_list = [('accuracy', accuracy), ('precison', precison),
                           ('recall', recall), ('f1', f1),
                           ('algrithm', algrithm)]

    save_data_to_cell(column_content_list, connection, table_name, row_key, 'others')
    df = get_data_from_cell(connection, table_name, row_key, 'others')
    print("input:")
    print(column_content_list)
    print("output:")
    print(df)


if __name__ == "__main__":
    table_name = 'test_cell'
    try:
        connection = happybase.Connection('127.0.0.1', 9090)
        connection.open()
        # create_HBase_table(connection, table_name, {str('test_Series'): dict(max_versions=10),
        #                                             str('test_DataFrame'): dict(max_versions=10),
        #                                             str('others'): dict(max_versions=10)})
        test_Series_cell()
        test_DataFrame_cell()
        test_others_cell()
    finally:
        if connection:
            connection.close()
