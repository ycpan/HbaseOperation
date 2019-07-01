from utils.hbase_operation import HBaseOperation
from utils.hbase_operation import get_data_from_cell
from utils.hbase_operation import save_data_to_cell
import happybase
import pandas as pd
import numpy as np


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
    # save_data_to_cell(column_content_list, connection, table_name, row_key, 'test_Series')
    hb.write_data(df, table_name, row_key, 'test_Series')
    df = hb.get_data(table_name, row_key, 'test_Series')
    print("output:")
    print(df)


def test_DataFrame_cell():
    row_key = 'svc.test.DataFrame'
    indexs = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'k']
    df = pd.DataFrame(np.random.randn(10, 5), columns=['a', 'b', '1', 'd', 'e'], index=indexs)
    df['c'] = 'hello world'
    df.at['a', 'f']=None
    # df.set_value('a', 'f', None)
    # df.set_value('a', 'f', None)
    # df['a']['f'] = None
    print("input:")
    print(df)
    column_content_list = [('test_DataFrame', df)]
    # save_data_to_cell(column_content_list, connection, table_name, row_key, 'test_DataFrame')
    # df = get_data_from_cell(connection, table_name, row_key, 'test_DataFrame')
    hb.write_data(df, table_name, row_key, 'test_DataFrame')
    df = hb.get_data(table_name, row_key, 'test_DataFrame')
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

    # save_data_to_cell(column_content_list, connection, table_name, row_key, 'others')
    # df = get_data_from_cell(connection, table_name, row_key, 'others')
    hb.write_data(column_content_list, table_name, row_key, 'others')
    df = hb.get_data(table_name, row_key, 'others')
    print("input:")
    print(column_content_list)
    print("output:")
    print(df)

def test_dic_cell():
    row_key = 'svc.test.dic'
    accuracy = 0.5
    precison = 0.8
    recall = 0.2
    f1 = 0.9
    algrithm = 'svc.svm'
    # column_content_list = [('accuracy', accuracy), ('precison', precison),
    #                        ('recall', recall), ('f1', f1),
    #                        ('algrithm', algrithm)]
    data = {'accuracy': accuracy, 'precison': precison,
                           'recall': recall, 'f1': f1,
                           'algrithm': algrithm}

    # save_data_to_cell(column_content_list, connection, table_name, row_key, 'others')
    # df = get_data_from_cell(connection, table_name, row_key, 'others')
    hb.write_data(data, table_name, row_key, 'others')
    df = hb.get_data(table_name, row_key, 'others')
    print("input:")
    print(data)
    print("output:")
    print(df)

if __name__ == "__main__":
    table_name = 'test_cell'
    try:
        # connection = happybase.Connection('43.247.185.201', 9034)
        hb = HBaseOperation('43.247.185.201', 9034)
        # connection.open()
        # create_HBase_table(connection, table_name, {str('test_Series'): dict(max_versions=10),
        #                                             str('test_DataFrame'): dict(max_versions=10),
        #                                             str('others'): dict(max_versions=10)})
        # hb.write_data()
        test_Series_cell()
        test_DataFrame_cell()
        test_others_cell()
        test_dic_cell()
    finally:
        # if connection:
        #     connection.close()
        print('end')
