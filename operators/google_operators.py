from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.postrgres_engine_hook import postgres_hook

# Google Sheets API libraries
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Data handling libraries
import pandas as pd
from sqlalchemy import text
from datetime import datetime

creds_path = '/home/julianoflaherty/.credenciales/client_secret_583206691909-5lqm591rsvbnc3ooagfpv6kiglsg9emo.apps.googleusercontent.com.json'

class Spreadsheet:

    def __init__(self, spreadsheetId, worksheet, header_row, index_name):
        self.spreadsheetId = spreadsheetId
        self.worksheet = worksheet
        self.header_row = header_row
        self.index_name = index_name

    def get_records(self, fields_to_import):
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

        creds = None

        # See if auth already exists
        if os.path.exists('/home/julianoflaherty/.credenciales/sheets_token.pickle'):
            with open('/home/julianoflaherty/.credenciales/sheets_token.pickle', 'rb') as token:
                creds = pickle.load(token)

        # If it doesn't let's the user authorize
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                ## UPDATE CREDENTIALS PATH!!
                flow = InstalledAppFlow.from_client_secrets_file(
                    creds_path,
                    SCOPES)
                creds = flow.run_local_server(port=0)

            # saves auth for future uses
            with open('/home/julianoflaherty/.credenciales/sheets_token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        service = build(serviceName='sheets', version='v4', credentials=creds)

        result = service.spreadsheets().values().get(spreadsheetId=self.spreadsheetId, range=self.worksheet).execute()

        data = result['values'][self.header_row - 1:]
        headers = {}
        columns_row = data.pop(0)
        i = 0
        while (i < len(columns_row)) and (len(headers) != len(fields_to_import)):
            if columns_row[i] in fields_to_import.keys():
                headers[columns_row[i]] = i
            i = i + 1

        record = []
        for row in data:
            aux = {}
            for key in fields_to_import:
                aux[fields_to_import[key]] = row[headers[key]]
            record.append(aux)

        if self.index_name != '':
            i = 0
            for row in record:
                row[self.index_name] = i
                i += 1
        return record

    def get_spreadsheet_data(self, fields_to_import, filter_by, booleanize, dates):

        def filter_data(contents, col):
            row = 0
            while row < len(contents):
                if (contents[row][col] == ''):
                    contents.__delitem__(row)
                else:
                    row += 1

        def booleanizer(contents, col):
            '''
            Transforma yes y no en Booleanos. N/A o cualquier otro input queda vacio
            :param contents: dict
            :param col: columna a booleanizar
            :return:
            '''
            for row in contents:
                if (row[col].lower() == 'yes') or (row[col].lower() == 'si') or (row[col].lower() == 'y') or (
                        row[col].lower() == 'true'):
                    row[col] = True
                elif (row[col].lower() == 'no') or (row[col].lower() == 'n') or (row[col].lower() == 'false'):
                    row[col] = False
                else:
                    row[col] = None

        def convert_dates(contents, col, format):

            for row in contents:
                row[col] = datetime.strptime(row[col], format)

        data_set = self.get_records(fields_to_import)

        if filter_by != '':
            filter_data(data_set, filter_by)

        for column in booleanize:
            booleanizer(data_set, column)
        for column in dates:
            try:
                convert_dates(data_set, column, dates[column])
            except ValueError:
                pass

        return data_set


class GoogleSheetsCopyTable(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id='',
                 database='',
                 schema='',
                 table_name='',
                 spreadsheetId='',
                 worksheet='',
                 header_row=1,
                 fields_to_import={},
                 filter_by='',
                 index_name=False,
                 booleanize=[],
                 dates={},
                 *args,
                 **kwargs
                 ):

        super(GoogleSheetsCopyTable, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.database = database
        self.schema = schema
        self.table_name = table_name
        self.spreadsheetId = spreadsheetId
        self.worksheet = worksheet
        self.header_row = header_row
        self.fields_to_import = fields_to_import
        self.filter_by = filter_by
        self.index_name = index_name
        self.booleanize = booleanize
        self.dates = dates

    def execute(self, context):
        record = Spreadsheet(spreadsheetId=self.spreadsheetId,
                             worksheet=self.worksheet,
                             header_row=self.header_row,
                             index_name=self.index_name,
                             ).get_spreadsheet_data(fields_to_import=self.fields_to_import,
                                                    filter_by=self.filter_by,
                                                    booleanize=self.booleanize,
                                                    dates=self.dates)
        table = pd.DataFrame.from_records(record)
        use_index = self.index_name != ''
        eng = postgres_hook(conn_id=self.conn_id, database=self.database).get_engine()
        if use_index:
            table.to_sql(name=self.table_name,
                         con=eng,
                         schema=self.schema,
                         if_exists='append',
                         index=self.index_name
                         )
        else:
            table.to_sql(name=self.table_name,
                         con=eng,
                         schema=self.schema,
                         if_exists='replace')


class GoogleSheetsUpdateTable(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id='',
                 database='',
                 schema='',
                 table_name='',
                 spreadsheetId='',
                 worksheet='',
                 header_row=1,
                 fields_to_import={},
                 filter_by='',
                 main_keys=[],
                 index_name='',
                 booleanize=[],
                 dates={},
                 *args,
                 **kwargs
                 ):

        super(GoogleSheetsUpdateTable, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.database = database
        self.schema = schema
        self.table_name = table_name
        self.spreadsheetId = spreadsheetId
        self.worksheet = worksheet
        self.header_row = header_row
        self.fields_to_import = fields_to_import
        self.filter_by = filter_by
        self.main_keys = main_keys
        self.index_name = index_name
        self.booleanize = booleanize
        self.dates = dates

    def update_modified(self, modified_data, main_keys):
        main_query = ''

        for i in modified_data:
            query = '''UPDATE {}.{} SET'''.format(self.schema, self.table_name)
            for key in i:
                if key in main_keys:
                    continue
                if i[key] != None:
                    if type(i[key]) == str and '\'' in i[key]:
                        formated = ''
                        for j in i[key]:
                            formated += j
                            if j == '\'':
                                formated += '\''
                        i[key] = formated
                    query += ''' {} = \'{}\','''.format(key, i[key])
                else:
                    query += ''' {} = NULL,'''.format(key)
            query = query[:len(query) - 1]
            query += ''' WHERE '''
            for key in main_keys:
                query += ''' {} = {}'''.format(key, i[key])
            query += ''';\n'''
            main_query += query

        return main_query

    def insert_new(self, new_data):
        main_query = ''
        sample = 'INSERT INTO {}.{}'.format(self.schema, self.table_name)

        field_order = '('
        for i in new_data[0]:
            field_order += str(i) + ', '
        sample += field_order[:len(field_order) - 2] + ') VALUES '

        for i in new_data:
            values = '('
            for key in i:
                if i[key] != None:
                    if type(i[key]) == str and '\'' in i[key]:
                        formated = ''
                        for j in i[key]:
                            formated += j
                            if j == '\'':
                                formated += '\''
                        i[key] = formated
                    values += '\'{}\', '.format(i[key])
                else:
                    values += 'NULL, '
            values = values[:len(values) - 2]
            values += ');\n'
            main_query += sample + values

        return main_query

    def execute(self, context):

        def sort_different(old_rec, new_rec, main_keys):
            def equal_dicts(a, b):
                for key in a:
                    if not (a[key] == b[key]):
                        break
                return a[key] == b[key]

            def main_keys_comparator(a, b, main_keys):
                for key in main_keys:
                    if not (a[key] == b[key]):
                        break
                return a[key] == b[key]

            new = []
            modified = []

            paired = []
            for i in new_rec:
                j = 0
                while (j < len(old_rec)) and not (main_keys_comparator(i, old_rec[j], main_keys)):
                    j += 1
                    while j in paired:
                        j += 1

                if j == len(old_rec):
                    new.append(i)
                else:
                    if not (equal_dicts(i, old_rec[j])):
                        modified.append(i)

            return new, modified

        eng = postgres_hook(conn_id=self.conn_id, database=self.database).get_engine()
        base = pd.read_sql_table(table_name=self.table_name, con=eng, schema=self.schema)
        sheet_record = Spreadsheet(spreadsheetId=self.spreadsheetId, worksheet=self.worksheet,
                                   header_row=self.header_row, index_name=self.index_name).get_spreadsheet_data(
            fields_to_import=self.fields_to_import,
            filter_by=self.filter_by,
            booleanize=self.booleanize,
            dates=self.dates
        )

        base_record = base.to_dict(orient='records')

        new, modified = sort_different(base_record, sheet_record, self.main_keys)

        if len(modified) > 0:
            update_query = self.update_modified(modified, self.main_keys)
        else:
            update_query = ''
        if len(new) > 0:
            new_query = self.insert_new(new)
        else:
            new_query = ''

        print(new_query + update_query)
        if len(new_query + update_query) > 1:
            sql = text(new_query + update_query)
            with eng.connect() as con:
                con.execute(sql)

        else:
            print('No updates detected')
