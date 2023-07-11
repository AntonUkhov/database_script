import pymysql
import psycopg2
from config import MYSQL_HOST, MYSQL_NAME, MYSQL_PASS, MYSQL_USER, PG_HOST, PG_PASS, PG_NAME, PG_USER, PG_PORT
import datetime
import time
import ipaddress

def tr_black_list(amount):
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
            my_cursor.execute("SELECT * FROM `black_list`")
            rows = my_cursor.fetchmany(amount)
            st = time.time()
            string = ''
            
            #while rows:
            while rows:
                for row in rows:
                    uid = row[1]
                    contact_uid = row[2]
                    string += f"""(
                         '{uid}',
                         '{contact_uid}'
                        ),"""
                # insert data into table postgres
            
                string = string[:len(string) - 1]
                try:
                    pg_cursor.execute(
                        f"""BEGIN;
                            SET CONSTRAINTS ALL DEFERRED;
                            INSERT INTO mcams_blacklist (
                            uid_id, contact_uid_id
                            ) VALUES {string};
                            DELETE FROM mcams_blacklist
                            WHERE mcams_blacklist.uid_id IS NOT NULL AND NOT EXISTS (
                            SELECT 1
                            FROM mcams_user
                            WHERE mcams_user.uid = mcams_blacklist.uid_id
                            ) OR NOT EXISTS (
                            SELECT 1
                            FROM mcams_user
                            WHERE mcams_user.uid = mcams_blacklist.contact_uid_id 
                            );
                            COMMIT;""")
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