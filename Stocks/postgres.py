import psycopg2

DB_NAME = 'financial_data'


def connect():
    conn = None

    try:
        conn = psycopg2.connect(
            host='localhost',
            database=DB_NAME,
            user='postgres',
            password='postgres',
            port=5433
        )

        cursor = conn.cursor()

        print('PostgreSQL database version:')
        cursor.execute('SELECT version()')

        db_version = cursor.fetchone()
        print(db_version)

        cursor.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed')

if __name__ == '__main__':
    connect()