import sys
from connect import connect
import psycopg2
import pandas as pd
import yfinance as yf

# Tools to help with implementation of database functions in stock.py
# Needs tidying up! 


def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False

def isint(value):
    try:
        int(value)
        return True
    except:
        return False

def isbool(value):
    bool_values = ['yes', 'true', 'no', 'false']
    return str(value).lower() in bool_values

def convert_type(value):

    if type(value) == int:
        return value
    elif type(value) == float:
        return value
    elif type(value) == bool:
        return value
    elif type(value) == list:
        return str(value)

    # Check edge cases
    elif isint(value):
        return int(value)
    elif isfloat(value):
        return float(value)
    elif isbool(value):
        # somewhat redunant code, also repetative
        true_values = ['yes', 'true']
        return value in true_values

    elif type(value) == str:
        return value
    else:
        return None

def get_postgres_type(value):
    '''
    Determine the postgresql type of a value
    '''

    # name: storage (bytes)
    integer_types = {
        'smallint': [-32768, 32767],
        'integer': [-2147483648, 2147483647],
        'bigint': [-9223372036854775808, 9223372036854775807]
    }

    # name: decimal precision
    # 2*32 is an arbitrarily large number
    float_types = {
        'decimal': 2*32,
        'numeric': 2*32,
        'real': 6,
        'double precision': 15
    }

    value = convert_type(value)

    # Integer types
    if type(value) == int:
        # Sort integer dictionary by size range
        integer_types = dict(sorted(integer_types.items(), 
                                    key=lambda item: max(item[1]) + min(item[1])))
        # Determine smallest suitable type
        for key, item in integer_types.items():
            if value <= max(item) and value >= min(item):
                return key
        # If no types in range
        return None
    
    # Float type
    elif type(value) == float:
        # Get number of decimal places
        decimal_places = lambda x: str(x)[::-1].find('.')
        # Sort float dictionary by size
        float_types = dict(sorted(float_types.items(),
                                  key=lambda item: item[1]))
        # Determine smallest suitable type
        for key, item in float_types.items():
            if decimal_places(value) <= item:
                return key 
        # If no types in range
    
    # String type
    elif type(value) == str:
        if len(value) < 255:
            return 'varchar(255)'
        else:
            return 'text'
    
    # List type
    elif type(value) == list:
        # Change later
        return 'text'
    
    elif type(value) == bool:
        return 'boolean'

    else:
        return 'text'


def get_column_types(table_name) -> dict:
    
    conn = None
    
    try:

        conn = connect()
        cursor = conn.cursor()

        # get list of all columns
        columns_query = f"""
                        SELECT column_name, data_type, character_maximum_length
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}';
        """
        cursor.execute(columns_query)
        columns = cursor.fetchall()

        data_types = dict()
        for column in columns:
            if column[1] == 'character varying':
                if type(column[2]) == int:
                    data_types[column[0]] = f'varchar({column[2]})'
            else:
                data_types[column[0]] = column[1]
        
        return data_types

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

def get_compatible_types(type1, type2):

    # !this is repeated
    # name: storage (bytes)
    integer_types = {
        'smallint': [-32768, 32767],
        'integer': [-2147483648, 2147483647],
        'bigint': [-9223372036854775808, 9223372036854775807]
    }

    # name: decimal precision
    # 2*32 is an arbitrarily large number
    float_types = {
        'decimal': 2*32,
        'numeric': 2*32,
        'real': 6,
        'double precision': 15
    }

    if type1 == type2:
        return type1
    # If both are integer types
    elif type1 in integer_types.keys() and type2 in integer_types.keys():
        bounds1 = integer_types[type1]
        delta1 = abs(bounds1[0]) + abs(bounds1[1])

        bounds2 = integer_types[type2]
        delta2 = abs(bounds2[0]) + abs(bounds2[1])

        if delta1 <= delta2:
                return type2
        else:
            # This shouldn't happen since function
            # is called only on incompatible types
            return type1
    # If both are float types
    elif type1 in float_types.keys() and type2 in float_types.keys():
        decimal_places1 = float_types[type1]
        decimal_places2 = float_types[type2]

        if decimal_places1 <= decimal_places2:
            return type2
        else:
            return type1
    # If one float type, one integer type
    elif type1 in float_types.keys() and type2 in integer_types.keys():
        return type1
    elif type1 in integer_types.keys() and type2 in float_types.keys():
        return type2
    #
    else:
        return 'text'

def adjust_database_columns_2(data: pd.DataFrame, table_name: str):
    """Adjust database columns to account for differing datatypes
    and new columns

    Iterate through each index in column
    Get types of each index in column
    Remove NoneTypes
    Get most compatible type
    Change table type

    
    """
    pass

    # for column in list(data.columns):
    #     column_types = [type(value) for value in data[column]]

    #     # If all entries are none, do nothing with column
    #     print(column)
    #     print(column_types)

    #     print(all([col == type(None) for col in column_types]))


def adjust_database_columns(data: dict, table_name: str):
    """Adjust database columns to account for differing
    datatypes and new columns

    To implement: make work for dataframe

    Parameters:
    ----------
    data (dict) : data to add to db
    table_name (str) : name of table to change

    """

    # Get types of columns of database and postgresql types of values in data

    # Get columns and types from database
    column_types = get_column_types(table_name)
    # Get keys and types from data
    info_types = {key: get_postgres_type(value) for key, value in data.items()}
    
    # Compute columns not in database
    excluded_columns = dict()
    for key, value in info_types.items():
        if key not in column_types.values():
            excluded_columns[key] = value

    try:
        # Connect to db
        conn = connect()
        cursor = conn.cursor()

        # Add excluded columns
        for key, value in excluded_columns.items():
            add_column_query = f"""
                                ALTER TABLE {table_name}
                                ADD COLUMN IF NOT EXISTS "{key}" {value};
            """
            cursor.execute(add_column_query)

        # Get number of rows in table
        row_count_query = f"""SELECT COUNT(*) FROM {table_name}"""
        cursor.execute(row_count_query)
        row_count = cursor.fetchall()[0][0]


        # Compute which columns contain only null values
        null_columns = []
        for column_name in column_types.keys():
            # Count the number of null values
            null_query = f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE "{column_name}" IS NULL
            """
            cursor.execute(null_query)
            null_count = cursor.fetchall()[0][0]
            # Check that the number of rows is the same as the number of null values
            if row_count == null_count:
                null_columns.append(column_name)
        
        #
        columns_to_alter = dict()

        # Compute which null_columns are in keys of data
        included_null_columns = [col for col in null_columns if col in data.keys()]

        for column in excluded_columns.keys():
            columns_to_alter[column] = info_types[column]

        # Get columns with incompatible datatypes
        incompatible_cols = []
        for col_name in column_types.keys():
            if info_types.get(col_name):
                if info_types[col_name] != column_types[col_name]:
                    incompatible_cols.append(col_name)

        # Get incompatible cols
        for col_name in incompatible_cols:
            col_type = column_types[col_name]
            info_type = column_types[col_name]

            columns_to_alter[col_name] = get_compatible_types(col_type, info_type)
        

        # repeated code: join together
        for col_name, type_name in columns_to_alter.items():
            # Update datatype
            new_data_type_query = f"""
                ALTER TABLE {table_name}
                ALTER COLUMN "{col_name}" TYPE {type_name}
                USING "{col_name}"::{type_name}
            """
            cursor.execute(new_data_type_query)

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
