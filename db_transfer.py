import pymysql
import psycopg2
from config import MYSQL_HOST, MYSQL_NAME, MYSQL_PASS, MYSQL_USER, PG_HOST, PG_PASS, PG_NAME, PG_USER


try:
    #connect to MySQL
    mysql_conn = pymysql.connect(
        host=MYSQL_HOST,
        port=3306,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_NAME,
        cursorclass=pymysql.cursors.SSCursor
    )
    print('Connection to MySQL success')
    print("#" * 20)

    #Connect to PostgeSQL
    pg_conn = psycopg2.connect(
        host=PG_HOST,
        user=PG_USER,
        password=PG_PASS,
        database=PG_NAME
    )
    pg_conn.autocommit = True
    print('Connection to PostgreSQL success')
    print("#" * 20)

    try:
        #select all data from table mysql
        with mysql_conn.cursor() as my_cursor:
            # bad_fraud
            my_cursor.execute("SELECT * FROM `bad_fraud`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                with pg_conn.cursor() as pg_cursor:
                    value = row[1]
                    pg_cursor.execute(f"INSERT INTO mcams_badfraud (bad_fraud) VALUES ('{value}')")
                row = my_cursor.fetchone()

            # bad_fraud_words
            my_cursor.execute("SELECT * FROM `bad_fraud_words`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                with pg_conn.cursor() as pg_cursor:
                    value = row[1]
                    pg_cursor.execute(f"INSERT INTO mcams_badfraudwords (bad_fraud_words) VALUES ('{value}')")
                row = my_cursor.fetchone()

            # bad_spam
            my_cursor.execute("SELECT * FROM `bad_spam`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                with pg_conn.cursor() as pg_cursor:
                    value = row[1]
                    pg_cursor.execute(f"INSERT INTO mcams_badspam (bad_spam) VALUES ('{value}')")
                row = my_cursor.fetchone()

            # bad_words
            my_cursor.execute("SELECT * FROM `bad_words`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                with pg_conn.cursor() as pg_cursor:
                    value = row[1]
                    pg_cursor.execute(f"INSERT INTO mcams_badwords (bad_words) VALUES ('{value}')")
                row = my_cursor.fetchone()

    finally:
        if mysql_conn:
            mysql_conn.close()
        if pg_conn:
            pg_conn.close()
except Exception as ex:
    print('Connection refused...')
    print(ex)

