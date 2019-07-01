from happybase import Connection
from utils.save_data import save_data_to_cell
from utils.get_data import get_data_from_cell


class HBaseOperation(Connection):
    def __init__(self, host, port=9090):
        self.host = host
        self.port = port
        super(HBaseOperation, self).__init__(self.host, self.port)

    def _re_init(self):
        import time
        time.sleep(1)
        self.close()
        super(HBaseOperation, self).__init__(self.host, self.port)
        self.open()

    def _write_data(self, data_tuple_list, table_name, row_key, cf):
        count = 2
        while count > 0:
            try:
                self._re_init()
                return save_data_to_cell(self, data_tuple_list, table_name, row_key, cf)
            except BrokenPipeError:
                count = count - 1
        raise OSError('retries reach max limits, retry failure')

    def write_data(self, input_data, table_name, row_key, cf):
        """
        :param input_data:
        :type Dataframe, Series, dic, str,  Binary data or data tuple.
        data tuple list:[(describe str, data)]
        :param table_name: 
        :param row_key: 
        :param cf: 
        :return: 
        """
        try:
            return save_data_to_cell(self, input_data, table_name, row_key, cf)
        except BrokenPipeError:
            print('Exception:save data happend BrokenPipeError, retry it')
            return self._write_data(input_data, table_name, row_key, cf)

    def _get_data(self, table_name, row_key, cf):
        count = 2
        while count > 0:
            try:
                self._re_init()
                return get_data_from_cell(self, table_name, row_key, cf)
            except BrokenPipeError:
                count = count - 1
        raise OSError('retries reach max limits, retry failure')

    def get_data(self, table_name, row_key, cf):
        try:
            return get_data_from_cell(self, table_name, row_key, cf)
        except BrokenPipeError:
            print('Exception:get data happend BrokenPipeError, retry it')
            return self._get_data(table_name, row_key, cf)

    @staticmethod
    def img_to_bin(canvas):
        """
        :param canvas:
        fig = plt.figure()
        plt.plot(x, y)
        canvas = fig.canvas
        :return:img bin data
        """
        import io
        buffer = io.BytesIO()
        canvas.print_png(buffer)
        data = buffer.getvalue()
        return data

    @staticmethod
    def bin_to_img(bin_data, name):
        open('{}'.format(name), 'wb').write(bin_data)

    def save_img_for_pdf(self, canvas, table_name, row_key, cf):
        from utils.pdf_report import PDFReport
        data = PDFReport.img_to_data(canvas)
        self.write_data(str(data), table_name, row_key, cf)

    def get_img_for_pdf(self, table_name, row_key, cf):
        data = self.get_data(table_name, row_key, cf)
        if data is None:
            return None
        return data['default']
