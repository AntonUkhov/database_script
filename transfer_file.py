import pymysql
import psycopg2
from config import MYSQL_HOST, MYSQL_NAME, MYSQL_PASS, MYSQL_USER, PG_HOST, PG_PASS, PG_NAME, PG_USER, PG_PORT
import datetime
import time
import ipaddress

def tr_file(amount):
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
        #mysql_conn.settimeout(200)

        # Connect to PostgeSQL
        pg_conn = psycopg2.connect(
            host=PG_HOST,
            user=PG_USER,
            password=PG_PASS,
            database=PG_NAME,
            port=PG_PORT
        )
        pg_conn.autocommit = True


        try:
            my_cursor = mysql_conn.cursor()
            pg_cursor = pg_conn.cursor()
            my_cursor.execute("SELECT * FROM `user_uploads`")
            rows = my_cursor.fetchmany(amount)
            st = time.time()
            string = ''
            
            #while rows:
            while rows:
                profile_pictures = []
                for row in rows:
                    file_id = row[0]
                    owner_id = row[1]
                    name = row[11]
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
                    check_file = row[5] + 1
                    check_profile_image = row[7]
                    if check_profile_image:
                        check_profile_image = 2
                    else:
                        check_profile_image = 1   
                    hash = row[9]
                    file_link = ''
                    if name == 'social':
                        file_link = row[2]
                        small_file = ''
                        middle_file = ''
                        large_file = ''
                    
                    if row[6]:
                        profile_pictures.append((owner_id, large_file))
                    
                    string += f"""(
                            '{file_id}',
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
                        ),"""
                string = string[:len(string) - 1]
                try:
                    pg_cursor.execute(
                        f"""BEGIN;
                            SET CONSTRAINTS ALL DEFERRED;
                            INSERT INTO mcams_file (
                            id,
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
                            ) VALUES {string};
                            DELETE FROM mcams_file
                            WHERE NOT EXISTS (
                            SELECT 1
                            FROM mcams_user
                            WHERE mcams_user.uid = mcams_file.owner_id
                            );
                            COMMIT;""")
                except Exception as ex:
                     print(ex)
                try:
                    for picture in profile_pictures:
                        pg_cursor.execute(
                        f"""UPDATE mcams_user SET profile_picture_id = (SELECT id from mcams_file WHERE owner_id = '{picture[0]}' and large_file = '{picture[1]}' LIMIT 1) WHERE uid = '{picture[0]}'""")
                except Exception as ex:
                    print(ex)
                rows = my_cursor.fetchmany(amount)
                string = ''
        except Exception as ex:
            print(ex)


    except Exception as ex:
        print('Connection refused...')
        print(ex)
    finally:
        if mysql_conn:
            mysql_conn.close()
        if pg_conn:
            pg_conn.close()