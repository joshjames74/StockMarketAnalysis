from os.path import exists
from functools import cached_property
import datetime
from dateutil import parser

import yfinance as yf
import pandas as pd
import sqlite3
import pytz
import sys
import psycopg2
import time

# Created libraries
from connect import connect
import time_tools
# Refactor: just import db_tools and call functions as methods
from db_tools import get_postgres_type, get_column_types, convert_type, get_compatible_types, adjust_database_columns

# Issue: stop using f strings
# Issue: timestamps saved as utc + 1
# Issue: rollback not possible on requests when in separate function
# (for log_request_to_database)

class Stock:
    '''
    '''

    def __init__(self, ticker: str, request_table : str = 'request', 
                 stock_price_table : str = 'stock_price',
                 stock_table : str = 'stock',
                 stock_financials_table : str = 'stock_financials',
                 stock_actions_table : str = 'stock_actions',
                 stock_holders_table : str = 'stock_holders',
                 update_info_delta : datetime.timedelta = datetime.timedelta(days=0)) -> None:
        """Initialise instance of Stock class

        Validate and set parameters.

        Create (if not already created) request, stock, and stock_data databases.

        Parameters:
        ----------
        ticker (str) : name of the stock ticker
        request_table (str) : name of the request table
        stock_price_table (str) : name of the stock price table
        stock_table (str) : name of the stock table
        stock_financials_table (str) : name of the stock financials table
        update_info_delta (datetime.timedelta) : time within which to not update stock info

        Returns:
        ---------
        None

        Raises:
        ----------
        TypeError : parameter is not a string
        ValueError : parameter is an empty string
        """

        string_parameters = [ticker, request_table, stock_price_table, 
                             stock_table, stock_financials_table, stock_actions_table,
                             stock_holders_table]

        # Check types of parameters
        for string_param in string_parameters:
            if not isinstance(string_param, str):
                raise TypeError(f'{string_param} is not a string (type: {type(string_param)})')
        
        if not isinstance(update_info_delta, datetime.timedelta):
            raise TypeError(f'{update_info_delta} is not an instance of class datetime.timedelta' \
                            f'(type: {type(update_info_delta)})')
        
        # Check that parameters are not empty strings
        for string_param in string_parameters:
            if not string_param:
                raise ValueError(f'{string_param} is an empty string')

        # Set fixed (or not changed by program) attributes
        self._ticker = ticker
        self._stock = yf.Ticker(self._ticker)

        # Set attributes
        self.request_table = request_table
        self.stock_price_table = stock_price_table
        self.stock_table = stock_table
        self.stock_financials_table = stock_financials_table
        self.stock_actions_table = stock_actions_table
        self.stock_holders_table = stock_holders_table
        self.update_info_delta = update_info_delta

        conn = None
        
        try:
            # Connect to db
            conn = connect()
            cursor = conn.cursor()

            # Create stock table
            stock_table_query = f"""
                        CREATE TABLE IF NOT EXISTS {self.stock_table}
                        (
                            id serial PRIMARY KEY,
                            ticker VARCHAR (255) UNIQUE NOT NULL
                        );
            """
            cursor.execute(stock_table_query)
            print(f'Created {stock_table} table (if not exists)')

            # Create requests table
            request_table_query = f"""
                        CREATE TABLE IF NOT EXISTS {self.request_table}
                        (
                            id serial PRIMARY KEY,
                            stock_id integer,
                            utc_time timestamp with time zone,
                            request_type varchar(255),
                            FOREIGN KEY (stock_id)
                                REFERENCES {self.stock_table} (id)
                                ON DELETE CASCADE
                        );
            """
            cursor.execute(request_table_query)
            print(f'Created {request_table} table (if not exists)')

            
            # Create stock price table
            stock_price_table_query = f"""
                        CREATE TABLE IF NOT EXISTS {self.stock_price_table}
                        (
                            id serial PRIMARY KEY,
                            request_id integer,
                            utc_time timestamp with time zone,
                            open double precision,
                            high double precision,
                            low double precision,
                            close double precision,
                            adj_close double precision,
                            volume bigint,
                            FOREIGN KEY (request_id)
                                REFERENCES {self.request_table} (id)
                                ON DELETE CASCADE
                        );
            """
            cursor.execute(stock_price_table_query)
            print(f'Created {stock_price_table} table (if not exists)')

            # Create stock financials table
            stock_financials_table_query = f"""
                        CREATE TABLE IF NOT EXISTS {self.stock_financials_table}
                        (
                            id serial PRIMARY KEY,
                            stock_id integer,
                            FOREIGN KEY (stock_id)
                                REFERENCES {self.stock_table} (id)
                                ON DELETE CASCADE
                        );
            """
            cursor.execute(stock_financials_table_query)
            print(f'Created {self.stock_financials_table} table (if not exists)')

            stock_actions_table_query = f"""
                        CREATE TABLE IF NOT EXISTS {self.stock_actions_table}
                        (
                            id serial PRIMARY KEY,
                            stock_id integer,
                            FOREIGN KEY (stock_id)
                                REFERENCES {self.stock_table} (id)
                                ON DELETE CASCADE
                        );
            """
            cursor.execute(stock_actions_table_query)
            print(f'Created {self.stock_actions_table} table (if not exists')

            stock_holders_table_query = f"""
                        CREATE TABLE IF NOT EXISTS {self.stock_holders_table}
                        (
                            id serial PRIMARY KEY,
                            stock_id integer,
                            FOREIGN KEY (stock_id)
                                REFERENCES {self.stock_table} (id)
                                ON DELETE CASCADE
                        )
            """
            cursor.execute(stock_holders_table_query)
            print(f'Create {self.stock_holders_table} table (if not exists)')

        except (Exception, psycopg2.DatabaseError) as error:
            # Rollback changes if an error occurs
            conn.rollback()
            raise Exception(error)
        finally:
            # Commit and close
            if conn:
                conn.commit()
                conn.close()
                print('Database connection is closed')

    @property
    def ticker(self) -> str:

        """Getter method for _ticker property

        Returns:
        ----------
        str : name of the stock ticker
        """
        return self._ticker

    @ticker.setter
    def ticker(self, new_ticker: str) -> None:
        """Setter method for _ticker property

        Parameters:
        ----------
        new_ticker (str) : name of the new stock_ticker

        Raises:
        ----------
        TypeError : new_ticker is not a string
        ValueError: new_ticker contains one or less characters
        """
        # Check new_ticker is a string
        if not isinstance(new_ticker, str):
            raise TypeError(f'{new_ticker} is not a string (type: {type(new_ticker)})')
        # Check new_ticker is not empty
        if not new_ticker:
            raise ValueError(f'{new_ticker} is an empty string')

        # Set _ticker property
        self._ticker = new_ticker

    @property
    def stock(self) -> yf.Ticker:
        """Getter method for _stock property

        Returns:
        ----------
        yf.Ticker : yfinance ticker object
        """
        return self._stock

    @staticmethod
    def time_to_utc(time: datetime.datetime) -> datetime.datetime:
        ''' Convert datetime to string of utc time
        '''
        utc_time = time.astimezone(pytz.UTC)
        return utc_time

    @cached_property
    def last_stock_info_update(self) -> datetime.datetime:
        """Getter method for last_stock_info_update
        
        Determine when the stock_info was last updated, using the 
        requests database.

        Returns:
        ----------
        last_update (datetime.datetime) : datetime object of last update
        """

        conn = None
        last_update = None

        try:
            # Connect to db
            conn = connect()
            cursor = conn.cursor()

            # Check that stock exists in the database
            if not self.stock_id:
                raise Exception('stock_id not found')
            
            # Query db for utc_time of last update
            stock_update_query = f"SELECT utc_time FROM {self.request_table} WHERE stock_id = {self.stock_id};"
            cursor.execute(stock_update_query)

            return_value = cursor.fetchall()
            
            if return_value:
                last_update = return_value[0][0]

        except (Exception, psycopg2.DatabaseError) as error:
            raise Exception(error)
        finally:
            # Commit and close
            if conn:
                conn.commit()
                conn.close()
            return last_update
    
    @cached_property
    def stock_id(self) -> int:
        """Getter method for stock_id

        Query the stock database to find the id of the stock with
        ticker equal to the ticker attribute.

        If stock is not yet in database (or otherwise cannot be found)
        then return None

        Returns
        ----------
        If stock is not yet in database (or otherwise cannot be found)
        then return None

        stock_id (int) : id of the stock in the database
        """

        stock_id = None
        conn = None

        try:
            # Connect to db
            conn = connect()
            cursor = conn.cursor()

            # Get stock id from database
            stock_id_query = f"SELECT id FROM {self.stock_table} WHERE {self.stock_table}.ticker = '{self.ticker}';"
            cursor.execute(stock_id_query)
            returned_data = cursor.fetchall()

            # If data is returned, set stock_id
            if returned_data:
                stock_id = returned_data[0][0]

            # Insert row into stock database ----------

            # Add stock to db
            if not returned_data:
                # Add stock to db
                add_stock_query = f"INSERT INTO {self.stock_table} (ticker) VALUES ('{self.ticker}')"
                cursor.execute(add_stock_query)
                conn.commit()
                # Get id of added stock
                stock_id_query = f"SELECT currval(pg_get_serial_sequence('{self.stock_table}','id'));"
                cursor.execute(stock_id_query)
                stock_id = cursor.fetchone()[0]

        except (Exception, psycopg2.DatabaseError) as Error:
            raise Exception(error)
        finally:
            # Commit and close
            if conn:
                conn.commit()
                conn.close()
            return stock_id

    @cached_property
    def financials_reports_dates(self) -> list:
        """Getter method for finances_dates
        
        Returns:
        ----------
        list : list of dates of financial reports
        """

        conn = None
        financial_reports = None

        try:
            # Connect to db
            conn = connect()
            cursor = conn.cursor()

            reports_dates_query = f"""SELECT date
                                      FROM {self.stock_financials_table}
                                      WHERE "stock_id" = {self.stock_id}"""
            cursor.execute(reports_dates_query)
            return_values = cursor.fetchall()
            if return_values:
                financial_reports = [parser.parse(date[0]) for date in return_values]

        except (Exception, psycopg2.DatabaseError) as error:
            raise Exception(error)
        finally:
            if conn:
                conn.commit()
                conn.close()
            return financial_reports
    
    @cached_property
    def actions_dates(self) -> list:
        """Getter method for actions_dates

        Returns:
        ---------
        list : list of dates of actions in database
        """
        
        conn = None
        actions_dates = None

        try:
            conn = connect()
            cursor = conn.cursor()

            actions_dates_query = f"""SELECT date 
                                      FROM {self.stock_actions_table}
                                      WHERE "stock_id" = {self.stock_id}"""
            cursor.execute(actions_dates_query)

            return_values = cursor.fetchall()
            if return_values:
                actions_dates = [parser.parse(date[0]) for date in return_values]

        except (Exception, psycopg2.DatabaseError) as error:
            raise Exception(error)
        finally:
            if conn:
                conn.commit()
                conn.close()
            return actions_dates
    
    def get_available_stock_timestamps(self):
        '''
        '''

        conn = None
        timestamps = None

        try:
            # Connect to db
            conn = connect()
            cursor = conn.cursor()

            # Set query to get timestamps for given ticker
            query = f"""
                    SELECT {self.stock_price_table}.utc_time 
                        FROM {self.stock_price_table}
                        LEFT JOIN {self.request_table}
                        ON {self.request_table}.id = {self.stock_price_table}.request_id
                    WHERE {self.request_table}.ticker = '{self.ticker}'"""

            # Execute and retrieve data
            cursor.execute(query)
            timestamps = cursor.fetchall()

        except sqlite3.Error as error:
            print('Failed to create SQLite table', error)
        finally:
            # Commit and close
            if conn:
                conn.close()
                print('Database connection is closed')
                return timestamps

    def download_stock_price_data(self, period : str="1d", interval : str = "1m") -> None:
        """Download stock price data for given period

        Use yfinance retrieve stock price data and then store it in the stock_price_table

        Parameters:
        ----------
        period (str) : period over which to retrieve data
        interval (str) : interval between stock price data values
        """
        # Need to validate parameters

        # Get data from yfinance api
        data = yf.download(
            tickers=self.ticker,
            period=period,
            interval=interval
        )

        # Set datatypes for columns
        column_dtypes = {'Open': float, 
                         'High': float,
                         'Low': float,
                         'Close': float,
                         'Adj Close': float,
                         'Volume': int}

        # Convert columns to respective datatypes
        for name, dtype in column_dtypes.items():
            data[name] = data[name].astype(dtype)

        # Insert data into database
        self.insert_stock_price_to_database(data, period, interval)

    def download_stock_info(self) -> None:
        """Download stock info data

        Check that stock info data was not updated within update window

        Use yfinance to download stock info data and save to stock table
        """

        # Check if stock was recently updated
        if self.last_stock_info_update:
            next_update_available = self.last_stock_info_update + self.update_info_delta
            current_utc_time = datetime.datetime.now(datetime.timezone.utc)

            if next_update_available > current_utc_time:
                print(f"\n{self.ticker} data recently updated in database")
                print(f"Last update: {self.last_stock_info_update}")
                print(f"Next update available at: {self.last_stock_info_update + self.update_info_delta}")
                return
        
        # Get stock info
        data = self.stock.info

        data['ticker'] = data['symbol']
        del data['symbol']    

        data = pd.DataFrame([data])

        update_dict = {'column': 'id', 'value': self.stock_id}

        # adjust_database_columns_2(data, self.stock_table)

        # Insert to db
        self.add_dataframe_to_database(data, self.stock_table,
                                       name='update', identifier=update_dict)
        self.log_request_to_database('stock_info')
    
    def download_stock_financials(self) -> None:

        financials_df = self.stock.financials

        # Transpose dataframe
        financials_df = financials_df.transpose()
        # Convert column names to lowercase
        financials_df.columns = financials_df.columns.str.lower()
        # Replace spaces with underscore in column names
        financials_df.columns = financials_df.columns.str.replace(" ", "_")

        financials_df['date'] = financials_df.index.astype(str)
        financials_df['stock_id'] = [self.stock_id] * len(financials_df.index)

        # Remove any rows already in database
        if self.financials_reports_dates:
            for date in self.financials_reports_dates:
                financials_df = financials_df[financials_df['date'] != str(date.date())]   
            # If all rows are removed, return
            if financials_df.empty:
                print("Financials retrieved already exist in database")
                return
        
        # Remove rows where all values are None
        # for label, content in financials_df.items():
        #     print(content.to_list())
    
        # print(financials_df)

        # financials_df.replace("'", "")

        # adjust_database_columns_2(financials_df, self.stock_financials_table)

        self.add_dataframe_to_database(financials_df, self.stock_financials_table)
        self.log_request_to_database('stock_financials')

    def download_stock_actions(self) -> None:
        """Download stock actions
        """

        actions = self.stock.actions

        dates = actions.index.tolist()
        # dates = [parser.parse(date) for date in dates]

        # Remove already downloaded dates
        if self.actions_dates:
            for action_date in self.actions_dates:
                actions = actions[actions.index != action_date]
            if actions.empty:
                print('Actions retrieved already exist in database')
                return
        
        # Convert column names to lowercase
        actions.columns = actions.columns.str.lower()
        # Replace spaces with underscore in column names
        actions.columns = actions.columns.str.replace(" ", "_")
        
        actions['stock_id'] = [self.stock_id] * len(actions.index)
        actions['date'] = actions.index.astype(str)

        self.add_dataframe_to_database(actions, self.stock_actions_table)
        self.log_request_to_database('stock_actions')
    
    def download_stock_holders(self) -> None:
        # Need caching system for holders

        institutional_holders = self.stock.institutional_holders
        mutualfund_holders = self.stock.mutualfund_holders

        # Check that columns names are the same
        if list(institutional_holders.columns) != list(mutualfund_holders.columns):
            raise Exception('Column names of institutional and mutual fund holders not equal' \
                            f'Institutional holders columns names: {institutional_holders.columns}' \
                            f'Mutual fund holders columns names: {mutualfund_holders.columns}')
        
        # Set holder type
        institutional_holders['type'] = ['institutional'] * len(institutional_holders.index)
        mutualfund_holders['type'] = ['mutual'] * len(mutualfund_holders.index)

        # Join dataframes
        holders = institutional_holders.append(mutualfund_holders, ignore_index=True)
        # Convert column names to lowercase
        holders.columns = holders.columns.str.lower()
        # Replace spaces with underscore in column names
        holders.columns = holders.columns.str.replace(" ", "_")

        holders['date_reported'] = holders['date_reported'].astype(str)
        holders['stock_id'] = [self.stock_id] * len(holders.index)
        
        self.add_dataframe_to_database(holders, self.stock_holders_table)
        self.log_request_to_database('stock_holders')
    
    def insert_stock_price_to_database(self, data: pd.DataFrame, 
                                       period: str, interval: str) -> None:

        # Change download_stock_price_data so that this function
        # can be replaced by calling add_dataframe_to_database
        
        # Get time
        time = datetime.datetime.now()
        time = self.time_to_utc(time)
        time = str(time)

        conn = None

        # Insert request
        try:
            # Make connection
            conn = connect()
            cursor = conn.cursor()

            # Insert row into stock database ----------

            # Check if stock already in database
            stock_id = self.stock_id

            # Add stock to db
            if not stock_id:
                # Add stock to db
                add_stock_query = f"INSERT INTO {self.stock_table} (ticker) VALUES ('{self.ticker}')"
                cursor.execute(add_stock_query)
                conn.commit()
                # Get id of added stock
                stock_id_query = f"SELECT currval(pg_get_serial_sequence('{self.stock_table}','id'));"
                cursor.execute(stock_id_query)
                stock_id = cursor.fetchone()[0]

            # Insert row into request database ---------

            # Add request to db
            request_type = 'stock_price'
            row = (time, stock_id, period, interval, request_type)
            columns = '(utc_time, stock_id, period, interval, request_type)'
            request_query = f"INSERT INTO {self.request_table}{columns} VALUES {row}"

            cursor.execute(request_query)
            print(f'Inserted row into {self.request_table}')

            # Get row id for next query
            get_request_id_query = f"SELECT currval(pg_get_serial_sequence('{self.request_table}','id'));"
            cursor.execute(get_request_id_query)
            request_id = cursor.fetchone()[0]

            conn.commit()

            # Insert rows into stock table -----------

            columns_to_insert = '(request_id, utc_time, open, high, low, close, adj_close, volume)'

            # Add rows into table

            for row in data.iterrows():
                
                # Convert time to UTC and to string
                local_time = row[0]
                utc_time = self.time_to_utc(local_time)
                string_time = str(utc_time)

                # Separate series object at index 1
                row = row[1].values

                # Concatenate required values
                values = [request_id, string_time] + list(row)
                values = tuple(values)

                # Execute
                cursor.execute(f'INSERT INTO {self.stock_price_table}{columns_to_insert} VALUES {values};')
                conn.commit()
            
            print(f'Inserted #{len(data)} rows into {self.stock_price_table}')

        except (Exception, psycopg2.DatabaseError) as error:
            # Rollback changes if an error occurs
            conn.rollback()
            raise Exception(error)
        finally:
            # Commit and close
            if conn:
                conn.commit()
                conn.close()
                print('Database closed')

    def add_dataframe_to_database(self, df: pd.DataFrame, table_name: str, 
                                  name : str = 'insert', identifier : dict = {}) -> None:
        """
        Dataframe must have same column names as database
        """

        for index, row in df.iterrows():
            row = row.to_dict()
            row = {key: value for key, value in row.items() if value}
            adjust_database_columns(row, table_name)

        conn = None

        try:
            # Connect to db
            conn = connect()
            cursor = conn.cursor()

            columns = list(df.columns)
            columns = f"{tuple(columns)}"
            columns = columns.replace("'", '"')

            for index, row in df.iterrows():
                row = row.to_dict()
                row = {key: value for key, value in row.items() if value}

                # adjust_database_columns(row, table_name)

                values = f"{tuple(row.values())}"
                columns = f"{tuple(row.keys())}"

                columns = columns.replace("'", '"')

                query = ""

                if name == 'insert':
                    query = f"INSERT INTO {table_name}{columns} VALUES {values};"
                if name == 'update':
                    query = f"""UPDATE {table_name}
                                SET {columns} = {values}
                                WHERE {identifier['column']} = {identifier['value']}"""
                
                if query:
                    cursor.execute(query)
            
            print(f"Inserted {len(df.index)} rows into {table_name}")
            
        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            raise Exception(error)
        finally:
            if conn:
                conn.commit()
                conn.close()
                
    def log_request_to_database(self, request_type: str) -> None:
        """
        """

        conn = None

        try:
            # Connect to db
            conn = connect()
            cursor = conn.cursor()
            
            utc_time = datetime.datetime.now(datetime.timezone.utc)
            utc_time = str(utc_time)

            columns = "(stock_id, utc_time, request_type)"
            values = (self.stock_id, utc_time, request_type)

            request_query = f"INSERT INTO {self.request_table}{columns} VALUES {values};"
            cursor.execute(request_query)

            print(f"Inserted row into {self.request_table}")

        except (Exception, psycopg2.DatabaseError) as error:
            conn.rollback()
            raise Exception(error)
        finally:
            # Commiy and close
            if conn:
                conn.commit()
                conn.close()

    def delete_all_tables(self) -> None:
        """Delete all tables from db"""

        # In order of deletion
        tables = [self.stock_financials_table, self.stock_actions_table,
                  self.stock_holders_table, self.stock_price_table, 
                  self.request_table, self.stock_table]

        conn = None

        try:
            conn = connect()
            cursor = conn.cursor()

            for table in tables:
                query = f"DROP TABLE IF EXISTS {table}"
                cursor.execute(query)
                print(f"Deleted table {table}")
            
            print(f"\nDeleted all tables")

        except (Exception, psycopg2.DatabaseError) as error:
            # Rollback changes if an error occurs
            conn.rollback()
            raise Exception(error)
        finally:
            # Commit and close
            if conn:
                conn.commit()
                conn.close()
                print("Database closed")
