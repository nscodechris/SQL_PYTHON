
# ALLOWED TO EDIT VERSION!

# importent imports to be used in program

import psycopg2
import pandas as pd


# pd.set_option('display.max_rows', 500)
# pd.set_option('display.max_columns', 500)
# pd.set_option('display.width', 1000)


# host can be like: host='127.0.0.1'

# parent class for the database
class MyDataBase:
    def __init__(self, *args, **kwargs):
        self.new_headers = ','.join([str(elem) for elem in args])
        self.headings_sql = []
        self.values_sql = []

    # function that connects to the database
    def connect_database(self, database, user, password, host, port):
        conn = None

        try:
            conn = psycopg2.connect(
                database=database, user=user, password=password, host=host, port=port)
        except Exception as error:
            print(error)
        return conn

    def save_sql_query(self, cursor, conn):
        conn.commit()
        cursor.close()

    # function that returns a value count that is needed in the insert_loop function, so the for loop nows how many time
    # to itterate thrue the kwargs = headings and values
    def item_count_per_key(self, dict):

        key_count = 0
        for key, value in dict.items():
            key_count += 1
        # print(key_count)
        value_count = 0
        for value in dict:
            value_list = dict[value]
            count = len(value_list)
            value_count += count
        value_count_per_key = round(value_count / key_count)
        return value_count_per_key

    # the second function for inserting values to a table
    def insert_loop(self, table_name, i, dict,  **kwargs):
        for x in range(0, self.item_count_per_key(dict)):
            for key, value in kwargs.items():
                if key not in self.headings_sql:
                    self.headings_sql.append(key)
                if value not in self.values_sql:
                    self.values_sql.append(value[i])
            insert_query_headings = ','.join([str(elem) for elem in self.headings_sql])
            insert_query_values = ','.join([str(elem) for elem in self.values_sql])
            query = f"INSERT INTO {table_name} ({insert_query_headings}) VALUES ({insert_query_values});"
            # print(query)
            return query

    # the start function for inserting values to a table
    def start_insert(self, conn, table_name, dict, **kwargs):
        i = 0
        cursor = conn.cursor()
        while i != self.item_count_per_key(dict):
            cursor.execute(self.insert_loop(table_name, i, dict, **kwargs))
            # print(insert_loop(table_name, i, **kwargs))
            self.values_sql.clear()
            i += 1
        self.save_sql_query(cursor, conn)

    # function to create a table, needs connection, table name and the headers for the table
    def create_table_sql(self, conn, table_name, headers):
        if_table_exist_query = f"DROP TABLE IF EXISTS {table_name};"
        sql_create_table = f"CREATE TABLE {table_name} ({headers})"
        cursor = conn.cursor()
        cursor.execute(if_table_exist_query)
        cursor.execute(sql_create_table)
        self.save_sql_query(cursor, conn)

    # function to create the database, needs connection and a database name
    def create_database(self, conn, database_name):
        sql_create_table = f'CREATE DATABASE {database_name}'
        # print(sql_create_table)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql_create_table)
        self.save_sql_query(cursor, conn)

    # function to use Alter table, update, drop, and so on
    def write_sql(self, sql_code, conn):
        cursor = conn.cursor()
        sql_query = sql_code
        cursor.execute(sql_query)
        self.save_sql_query(cursor, conn)

    # trying to add values and create table from csv file
    def view_csv_file_pandas(self, file, conn):
        data = pd.read_csv(file)
        df = pd.DataFrame(data)
        print(df)
        # print(list(df.columns.values))
        # print(len(df.columns.values))
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS products;")
        conn.commit()
        query = '''
        CREATE TABLE products 
        (Date_Received text, Product_Name text, Sub_Product text, Issue text, Sub_issue text, Consumer_complaint_Narrative text,
        Company_public_response text, Company text, State_name text, zip_code text, Tags text, Consumer_consent_Provided text, Submitted_via text,
        Date_sent_to_company text, Company_response_to_consumer text, Timely_response text, Disputed text, Complaint_id text) 
        '''
        cursor.execute(query)
        conn.commit()
        header_new_list = ['Date_Received', 'Product_Name', 'Sub_Product','Issu' , 'Sub_issue', 'Consumer_complaint_Narrative',
        'Company_public_response', 'Company', 'State_name', 'zip_code', 'Tags' , 'Consumer_consent_Provided', 'Submitted_via',
        'Date_sent_to_company', 'Company_response_to_consumer', 'Timely_response', 'Disputed', 'Complaint_id']

        # adding values from pandas, that has imported values from csv file.
        for i, row in data.iterrows():
            sql = '''
            INSERT INTO products VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s)
            '''
            cursor.execute(sql, tuple(row))
            conn.commit()

    # function to view specific sql code
    def view_sql(self, conn, sql_code):
        cur = conn.cursor()
        cur.execute(sql_code)
        row = cur.fetchone()
        print("The b helmets", cur.rowcount)
        while row is not None:
            print(row)
            row = cur.fetchone()

# class creates a database
class CreateDatabase(MyDataBase):
    def __init__(self, database_name):
        super().__init__()
        conn = MyDataBase.connect_database(self, 'postgres', 'postgres', 'Cvmillan10!?', 'localhost', '5432')
        MyDataBase.create_database(self, conn, database_name)


# class creates a table and inserts value or just insert values to existing table
# if you want null value just write 'null' for other text write "'Emma'"
class TableOne(MyDataBase):
    def __init__(self, database_name, table_name):

        conn = MyDataBase.connect_database(self, database_name, 'postgres', 'Cvmillan10!?', 'localhost', '5432')

        heads_for_table = 'id SERIAL PRIMARY KEY', 'brand VARCHAR(30)', 'model VARCHAR(30)', 'price INT'
        values_for_table = {
                                'brand': ["'Monark'", "'Cross-bike'", "'TEST'"],
                                'model': ["'Emma'", "'Sigvard'", "'LISA'"],
                                'price': [7500, 8200, 932]}

        super().__init__(*heads_for_table, **values_for_table)
        MyDataBase.create_table_sql(self, conn, table_name, self.new_headers)
        MyDataBase.start_insert(self, conn, table_name, values_for_table, **values_for_table)
        # sql_query_view = '''SELECT * FROM bicycle_helmets;'''
        # MyDataBase.view_sql(self, conn, sql_query_view)


# class creates a table and inserts value or just insert values to existing table
# if you want null value just write 'null' for other text write "'Emma'"
class TableTwo(MyDataBase):
    def __init__(self,database_name, table_name):
        conn = MyDataBase.connect_database(self, database_name, 'postgres', 'Cvmillan10!?', 'localhost', '5432')
        heads_for_table = 'id SERIAL PRIMARY KEY', 'type VARCHAR(30)','model VARCHAR(30)', 'price INT'
        values_for_table = {
                            'type': ["'Hard'", "'Down-hill'", "'City'"],
                            'model': ["'Turtle'", "'Bird'", "'SUV'"],
                            'price': [700, 825, 450]}
        super().__init__(*heads_for_table, **values_for_table)
        MyDataBase.create_table_sql(self, conn, table_name, self.new_headers)
        MyDataBase.start_insert(self, conn, table_name, values_for_table, **values_for_table)


# class creates a table and inserts value or just insert values to existing table
# if you want null value just write 'null' for other text write "'Emma'"
class TableThree(MyDataBase):
    def __init__(self, database_name, table_name):
        conn = MyDataBase.connect_database(self, database_name, 'postgres', 'Cvmillan10!?', 'localhost', '5432')
        heads_for_table = 'id SERIAL PRIMARY KEY', 'brand VARCHAR(30)','model VARCHAR(30)','num_gears VARCHAR(30)', 'price INT'
        values_for_table = {
                            'brand': ["'Monark'", "'Cross-bike'", "'TEST'", "'Fat bike'"],
                            'model': ["'Emma'", "'Sigvard'", "'LISA'", 'null'],
                            'num_gears': [4, 5, 6, 7],
                            'price': [7, 8, 9, 6]}
        super().__init__(*heads_for_table, **values_for_table)
        MyDataBase.create_table_sql(self, conn, table_name, self.new_headers)
        MyDataBase.start_insert(self, conn, table_name, values_for_table, **values_for_table)


# create_database = CreateDatabase("bicycle_shop")
bicycle_shop_table_helmet = TableOne('bicycle_shop', 'bicycle_model')
bicycle_shop_table_models = TableTwo('bicycle_shop', 'bicycle_helmets')
bicycle_shop_table_stuff = TableThree('bicycle_shop', 'bicycle_stuff')

