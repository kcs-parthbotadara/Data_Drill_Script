import pandas as pd
import pymysql
import configparser
import os
from datetime import datetime, time, timedelta
from time import sleep
import uuid
import logging
from logging.handlers import TimedRotatingFileHandler
from send_email import sent_mail



class BatchProcessing:
    # Constructor
    def __init__(self, user, password, host, database, steps_to_move, number_of_batches, file_path, global_id_tag_name, column_names, batch_scheduling_time,
                 create_table, insert_table, select_table, show_table, truncate_table, drop_table, logger_file_name, email_from, email_password, email_to):
        # Initializing mysql_connection variables
        self.user = user
        self.password = password
        self.host = host
        self.database = database

        # Initializing batch_processing variables
        self.steps_to_move = steps_to_move
        self.number_of_batches = number_of_batches
        self.file_path = file_path
        self.global_id_tag_name = global_id_tag_name
        self.column_names = column_names
        self.batch_scheduling_time = batch_scheduling_time

        # Initializing sql_query variables
        self.create_table = create_table
        self.insert_table = insert_table
        self.select_table = select_table
        self.show_table = show_table
        self.truncate_table = truncate_table
        self.drop_table = drop_table

        # Initializing logger file name and object
        self.logger_file_name = logger_file_name
        self.logger = self.initialize_logger(self.logger_file_name)

        # Initializing email variables
        self.email_from = email_from
        self.email_password = email_password
        self.email_to = email_to

    # Create and configure logger file
    def initialize_logger(self, logger_file_name):
        logger = logging.getLogger("batch_processing_logger")
        logger.setLevel(logging.DEBUG)
        # handler = logging.FileHandler(logger_file_name)
        handler = TimedRotatingFileHandler(logger_file_name, when="D", interval=1)
        handler.suffix = "%Y-%m-%d_%H-%M-%S.log"
        formatter = logging.Formatter('%(asctime)s | %(process)s | %(levelname)s | %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    # Create connection to MySQl database
    def connection_mysql(self):
        try:
            self.logger.info("Execute function for connecting with MySQl")
            connection = pymysql.connect(user=self.user, passwd=self.password,
                                         host=self.host,
                                         database=self.database)

            cursor = connection.cursor()
            cursor.execute('set global connect_timeout=82800')
            self.logger.info("Connected with MySQl server")
            return connection, cursor
        except:
            self.logger.error("Error has occurred while connecting to MySQL")
            return "Error has occurred while connecting to MySQL"

    # Show Table
    def show_table_(self, cursor):
        try:
            table_lst = []
            cursor.execute(self.show_table)
            for x in cursor:
                table_lst.append(x)
            self.logger.info("Total number of tables database: %s", table_lst)
            return table_lst
        except:
            self.logger.error("Error while showing table")
            return 'Error while showing table'

    # Create Table
    def create_table_(self, cursor):
        try:
            # Execute create table query
            cursor.execute(self.create_table)
            self.logger.info("Table created successfully")
            # Show tables
            return self.show_table_(cursor)
        except:
            self.logger.error("Table already exists")
            return 'Table already exists'

    # Select Table - Check number of rows in table
    def select_table_(self, cursor):
        try:
            # Execute select table rows query
            cursor.execute(self.select_table)
            for row in cursor:
                self.logger.info("There are %s rows in the table", row[0])
                return row[0]
        except:
            self.logger.error("Error while selecting rows of table")
            return 'Error while selecting rows of table'

    # Truncate Table
    def truncate_table_(self, cursor):
        try:
            # Execute truncate table query
            cursor.execute(self.truncate_table)
            self.logger.info("Table truncated successfully")
            return 'Table truncated successfully'
        except:
            self.logger.error("Table truncation unsuccessful")
            return 'Table truncation unsuccessful'

    # Drop Table
    def drop_table_(self, cursor):
        try:
            # Execute truncate table query
            cursor.execute(self.drop_table)
            self.logger.info("Table dropped successfully")
            return 'Table dropped successfully'
        except:
            self.logger.error("Error while dropping table")
            return 'Error while dropping table'

    # Insert Rows to MySQL
    def insert_rows_to_mysql(self, cursor, count):
        try:
            # Check file is in CSV format
            if self.file_path.endswith('.csv'):
                self.logger.info("File is in csv format")
                if count == 0:
                    self.logger.info("Inserting first %s rows in the table", steps_to_move)
                    csv_data = pd.read_csv(self.file_path, skiprows=0, nrows=self.steps_to_move)
                    csv_data.fillna('', inplace=True)
                    csv_data.drop('Unnamed: 0', inplace=True, axis=1)
                    # csv_data.replace(np.nan, None, inplace=True)
                    csv_data['event_time'] = pd.to_datetime(csv_data['event_time'])
                    csv_data['event_time'] = pd.to_datetime(
                        csv_data['event_time'].dt.date.astype(str) + ' ' + csv_data['event_time'].dt.time.astype(str))

                    # Add global id column
                    csv_data.insert(loc=0, column='global_id', value='global_id')
                    csv_data['global_id'] = csv_data['global_id'].apply(lambda x: self.global_id_tag_name + '_' + str(uuid.uuid4()))

                    val = list(csv_data.itertuples(index=False, name=None))
                    cursor.executemany(self.insert_table, val)
                    self.logger.info('Inserted first %s to the table', steps_to_move)
                    status = 'Success'
                    inserted_rows = f'0-{count+self.steps_to_move}'
                    return status, inserted_rows
                else:
                    self.logger.info("Inserting next %s rows to the table",steps_to_move)
                    csv_data = pd.read_csv(self.file_path, skiprows=self.steps_to_move * count, nrows=self.steps_to_move)
                    csv_data.drop(csv_data.iloc[:, 0:1], inplace=True, axis=1)
                    csv_data.columns = self.column_names
                    csv_data.fillna('', inplace=True)
                    # csv_data.replace(np.nan, None, inplace=True)
                    csv_data['event_time'] = pd.to_datetime(csv_data['event_time'])
                    csv_data['event_time'] = pd.to_datetime(
                        csv_data['event_time'].dt.date.astype(str) + ' ' + csv_data['event_time'].dt.time.astype(str))

                    # Add global id column
                    csv_data.insert(loc=0, column='global_id', value='global_id')
                    csv_data['global_id'] = csv_data['global_id'].apply(lambda x: self.global_id_tag_name + '_' + str(uuid.uuid4()))

                    val = list(csv_data.itertuples(index=False, name=None))
                    cursor.executemany(self.insert_table, val)
                    self.logger.info("Inserted next %s to the table", steps_to_move)
                    status = 'Success'
                    inserted_rows = f'{self.steps_to_move * count}-{(self.steps_to_move * count)+self.steps_to_move}'
                    return status, inserted_rows
            # Check if file is in json or parquet format
            elif self.file_path.endswith('.parquet') or self.file_path.endswith('.json'):
                self.logger.info("Checking file is in parquet format or json format")
                parquet_or_json_data = pd.read_parquet(self.file_path)
                if count == 0:
                    self.logger.info("Inserting first %s rows in the table", steps_to_move)
                    parquet_or_json_data = parquet_or_json_data.iloc[:self.steps_to_move]
                    parquet_or_json_data.fillna('', inplace=True)
                    parquet_or_json_data.drop('Unnamed: 0', inplace=True, axis=1)
                    # parquet_data.replace(np.nan, None, inplace=True)
                    parquet_or_json_data['event_time'] = pd.to_datetime(parquet_or_json_data['event_time'])
                    parquet_or_json_data['event_time'] = pd.to_datetime(
                        parquet_or_json_data['event_time'].dt.date.astype(str) + ' ' + parquet_or_json_data['event_time'].dt.time.astype(str))

                    # Add global id column
                    parquet_or_json_data.insert(loc=0, column='global_id', value='global_id')
                    parquet_or_json_data['global_id'] = parquet_or_json_data['global_id'].apply(lambda x: self.global_id_tag_name + '_' + str(uuid.uuid4()))

                    val = list(parquet_or_json_data.itertuples(index=False, name=None))
                    cursor.executemany(self.insert_table, val)
                    self.logger.info("Inserted first %s rows in the table", steps_to_move)
                    status = 'Success'
                    inserted_rows = f'1-{count+self.steps_to_move}'
                    return status, inserted_rows
                else:
                    self.logger.info("Inserting next %s rows in the table", steps_to_move)
                    parquet_or_json_data = parquet_or_json_data.iloc[(self.steps_to_move * count):(self.steps_to_move * count) + self.steps_to_move]
                    parquet_or_json_data.drop(parquet_or_json_data.iloc[:, 0:1], inplace=True, axis=1)
                    parquet_or_json_data.columns = self.column_names
                    parquet_or_json_data.fillna('', inplace=True)
                    # parquet_data.replace(np.nan, None, inplace=True)
                    parquet_or_json_data['event_time'] = pd.to_datetime(parquet_or_json_data['event_time'])
                    parquet_or_json_data['event_time'] = pd.to_datetime(
                        parquet_or_json_data['event_time'].dt.date.astype(str) + ' ' + parquet_or_json_data['event_time'].dt.time.astype(str))

                    # Add global id column
                    parquet_or_json_data.insert(loc=0, column='global_id', value='global_id')
                    parquet_or_json_data['global_id'] = parquet_or_json_data['global_id'].apply(lambda x: self.global_id_tag_name + '_' + str(uuid.uuid4()))

                    val = list(parquet_or_json_data.itertuples(index=False, name=None))
                    cursor.executemany(self.insert_table, val)
                    self.logger.info("Inserted next %s rows in the table", steps_to_move)
                    status = 'Success'
                    inserted_rows = f'{self.steps_to_move * count}-{(self.steps_to_move * count)+self.steps_to_move}'
                    return status, inserted_rows
        except Exception as e:
            print(e)
            status = 'Failure'
            inserted_rows = '-'
            return status, inserted_rows

    # Get input from user
    def get_user_input(self, cursor, connection):
        self.logger.info("Selecting option from given points which action want to perform")
        try:
            # Taking input from user
            user_input = int(input('''
Please mention what you want to do:
1. Create table
2. Insert values into table
3. Get row count from table
4. Show table
5. Truncate table
6. Drop table
Please write input from (1 to 6)
'''))

            if user_input == 1:
                self.logger.info("selected option is create table, creating table.")
                # Create table
                created_table = self.create_table_(cursor)
                return created_table
            elif user_input == 2:
                self.logger.info("selected option is insert rows in table, Inserting rows in the table")
                connection.commit()
                if not self.file_path.endswith('.csv')  and not self.file_path.endswith('.parquet') and \
                        not self.file_path.endswith('.json'):
                    self.logger.error("File format is incorrect")
                    return "File format is incorrect"
                else:
                    # Insert table
                    for i in range(self.number_of_batches):
                        # Batch Scheduling - Batch will be processed every day at given time
                        start_time = time(*(map(int, self.batch_scheduling_time.split(':'))))
                        self.logger.info("Row will start inserting at %s", start_time)
                        # print(f'Batch {i + 1} will be processed at {start_time}')
                        # while (start_time > datetime.today().time().replace(second=0, microsecond=0)) or (start_time < datetime.today().time().replace(second=0, microsecond=0)):
                        #     continue
                        # sleep(60)
                        # Establishing MySQL connection
                        connection, cursor = self.connection_mysql()
                        # Inserting rows to MySQL in batches
                        status, inserted_rows = self.insert_rows_to_mysql(cursor, i)
                        print(f"Batches {i + 1} Processed")
                        # Send email
                        end_time = datetime.today().time().replace(second=0, microsecond=0)
                        table_name = self.create_table.split(' ')[2]
                        date = datetime.today().date()
                        message = sent_mail(email_from=self.email_from, email_password=self.email_password, email_to=self.email_to, batch_size=self.steps_to_move, start_time=start_time, end_time=end_time, table_name=table_name,
                                  batch_no=i + 1, file_name=self.file_path, date=date, inserted_rows=inserted_rows, status=status)
                        print(message)
                        if i == self.number_of_batches - 1:
                            break
                        # Closing connection
                        connection.commit()
                    self.logger.info("Data inserted successfully")
                    return "Data inserted successfully"

            elif user_input == 3:
                self.logger.info("Counting rows of the table")
                # Select table
                select_table = self.select_table_(cursor)
                return f"{select_table} Rows"
            elif user_input == 4:
                self.logger.info("Show total number of tables in database")
                # Show table
                show_table = self.show_table_(cursor)
                return show_table
            elif user_input == 5:
                self.logger.info("Truncating table")
                # Truncate table
                truncated_table = self.truncate_table_(cursor)
                return truncated_table
            elif user_input == 6:
                self.logger.info("Dropping table")
                # Drop table
                dropped_table = self.drop_table_(cursor)
                return dropped_table
            else:
                raise Exception
        except ValueError:
            self.logger.error("Invalid input. Enter only integer value from 1 to 6.")
            return "Please enter integer value from 1 to 6"
        except:
            self.logger.error("Please select from 1 to 6")
            return "Please select from 1 to 6"

    # Destructor
    def __del__(self):
        pass


# Get configuration values
def get_configuration(config_path):
    get_section = configparser.ConfigParser(interpolation=None)
    get_section.read(config_path)
    username = get_section.get('MYSQL_CONNECTION', 'username')
    password = get_section.get('MYSQL_CONNECTION', 'password')
    host = get_section.get('MYSQL_CONNECTION', 'host')
    database = get_section.get('MYSQL_CONNECTION', 'database')
    steps_to_move = get_section.getint('BATCH_PROCESSING', 'steps_to_move')
    number_of_batches = get_section.getint('BATCH_PROCESSING', 'number_of_batches')
    file_path = get_section.get('BATCH_PROCESSING', 'file_path')
    global_id_tag_name = get_section.get('BATCH_PROCESSING', 'global_id_tag_name')
    column_names = get_section.get('BATCH_PROCESSING', 'column_names')
    batch_scheduling_time = get_section.get('BATCH_PROCESSING', 'batch_scheduling_time')
    create_table = get_section.get('SQL_QUERY', 'create_table')
    insert_table = get_section.get('SQL_QUERY', 'insert_table')
    select_table = get_section.get('SQL_QUERY', 'select_table')
    show_table = get_section.get('SQL_QUERY', 'show_table')
    truncate_table = get_section.get('SQL_QUERY', 'truncate_table')
    drop_table = get_section.get('SQL_QUERY', 'drop_table')
    logger_file_name = get_section.get('LOGGER', 'logger_file_name')
    email_from = get_section.get('EMAIL', 'email_from')
    email_to = get_section.get('EMAIL', 'email_to')
    email_password = get_section.get('EMAIL', 'email_password')
    return username, password, host, database, steps_to_move, number_of_batches, file_path, global_id_tag_name, column_names, batch_scheduling_time, \
           create_table, insert_table, select_table, show_table, truncate_table, drop_table, logger_file_name, email_from, email_to, email_password


if __name__ == '__main__':
    # Get configuration
    config_path = f"{os.getcwd()}\configuration.conf"
    username, password, host, database, steps_to_move, number_of_batches, file_path, global_id_tag_name, column_names, batch_scheduling_time, create_table, \
        insert_table, select_table, show_table, truncate_table, drop_table, logger_file_name, email_from, email_to, email_password = get_configuration(config_path)

    # Converting string back to list
    column_names = column_names.strip('][').replace(' ', '').split(',')
    email_to = email_to.strip('][').replace(' ', '').split(',')

    # Initializing batch processing object
    batch_obj = BatchProcessing(username, password, host, database, steps_to_move, number_of_batches, file_path, global_id_tag_name, column_names, batch_scheduling_time,
                                create_table, insert_table, select_table, show_table, truncate_table, drop_table, logger_file_name, email_from, email_password, email_to)

    # Connection to MySQL
    connection, cursor = batch_obj.connection_mysql()

    # Get input from user and perform further operation
    while True:
        output = batch_obj.get_user_input(cursor, connection)
        print(output, '\n')
        input_ = input('Do you want to continue(yes or no)? ')
        if input_ == 'no':
            break

    # Closing connection
    connection.commit()
