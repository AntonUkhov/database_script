import pymysql
import psycopg2
from config import MYSQL_HOST, MYSQL_NAME, MYSQL_PASS, MYSQL_USER, PG_HOST, PG_PASS, PG_NAME, PG_USER
import datetime

try:
    # connect to MySQL
    mysql_conn = pymysql.connect(
        host=MYSQL_HOST,
        port=3306,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_NAME,
        cursorclass=pymysql.cursors.SSCursor
    )

    # Connect to PostgeSQL
    pg_conn = psycopg2.connect(
        host=PG_HOST,
        user=PG_USER,
        password=PG_PASS,
        database=PG_NAME
    )
    pg_conn.autocommit = True

    try:
        # select all data from table mysql
        with mysql_conn.cursor() as my_cursor, pg_conn.cursor() as pg_cursor:

            # file
            my_cursor.execute("SELECT * FROM `user_uploads`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                owner_id = row[1]
                row_for_old_db = f'uploads/{owner_id}/'
                small_file = ''
                middle_file = ''
                large_file = ''
                if row[4]:
                    small_file = row_for_old_db + row[4]
                if row[3]:
                    middle_file = row_for_old_db + row[3]
                if row[2]:
                    large_file = row_for_old_db + row[2]
                datetime = row[12]
                size = row[10]
                size = size / 1024
                type = row[8]
                if type == 'image':
                    type = 1
                else:
                    type = 2
                check_file = 1  # row[5]
                check_profile_image = row[6]
                if not check_profile_image:
                    check_profile_image = 1
                hash = row[9]
                file_link = ''
                try:
                    pg_cursor.execute(
                        f"""INSERT INTO mcams_file (
                            check_file,
                            owner_id,
                            small_file,
                            middle_file,
                            large_file,
                            datetime,
                            size,
                            type,
                            check_profile_image,
                            hash,
                            file_link
                            ) VALUES (
                            '{check_file}',
                            '{owner_id}',
                            '{small_file}',
                            '{middle_file}',
                            '{large_file}',
                            '{datetime}',
                            '{size}',
                            '{type}',
                            '{check_profile_image}',
                            '{hash}',
                            '{file_link}'
                            )""")
                except Exception as ex:
                    print(ex)
                row = my_cursor.fetchone()


    finally:
        if mysql_conn:
            mysql_conn.close()
        if pg_conn:
            pg_conn.close()
except Exception as ex:
    print('Connection refused...')
    print(ex)
