import psycopg2
from config import config

def connect():
    '''Connect to the PostgreSQL database server'''
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
        
        # return connectoin
        return conn

    except (Exception, psycopg2.DatabaseError) as error:
        raise Exception(error)

