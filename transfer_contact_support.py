import pymysql
import psycopg2
from config import MYSQL_HOST, MYSQL_NAME, MYSQL_PASS, MYSQL_USER, PG_HOST, PG_PASS, PG_NAME, PG_USER, PG_PORT
import datetime
import time
import ipaddress

def tr_contact_support(amount):
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
            my_cursor.execute("SELECT * FROM `contact_support`")
            rows = my_cursor.fetchmany(amount)
            st = time.time()
            string = ''
            
            #while rows:
            while rows:
                for row in rows:
                    # insert data into table postgres
                    if int(row[2]) == 0:
                        from_user = row[1]
                        to_user = 'support_uid'
                        support_name = row[3]
                    else:
                        from_user = 'support_uid'
                        to_user = row[1]
                        support_name = row[1]
                    message = row[4].replace("'", "''")
                    uploads_id_id = row[5]
                    if not uploads_id_id:
                        uploads_id_id = 'NULL'
                    else:
                        message = "📎"
                    read = row[8]
                    date_time = row[6]
                    sent = row[7]
                    link = 0
                    spam = 0
                    chat = True
                    support = True
                    
                    string += f"""(
                            '{from_user}',
                            '{to_user}',
                            '{support_name}',
                            '{message}',
                                {uploads_id_id},
                            '{read}',
                            '{date_time}',
                            '{sent}',
                            '{link}',
                            '{spam}',
                            '{chat}',
                            '{support}'
                        ),"""
                string = string[:len(string) - 1]
                try:
                    pg_cursor.execute(
                        f"""INSERT INTO mcams_messages (
                            from_user_id,
                            to_user_id,
                            support_name,
                            message,
                            uploads_id_id,
                            read,
                            date_time,
                            sent,
                            link,
                            spam,
                            chat,
                            support
                            ) VALUES {string};
                            DELETE FROM mcams_messages
                            WHERE mcams_messages.uploads_id_id IS NOT NULL AND NOT EXISTS (
                            SELECT 1
                            FROM mcams_file
                            WHERE mcams_file.id = mcams_messages.uploads_id_id
                            );
                            DELETE FROM mcams_messages
                            WHERE NOT EXISTS (
                            SELECT 1
                            FROM mcams_user
                            WHERE mcams_user.uid = mcams_messages.from_user_id 
                            ) OR NOT EXISTS (
                            SELECT 1
                            FROM mcams_user
                            WHERE mcams_user.uid = mcams_messages.to_user_id 
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

if __name__ == "__main__":
    tr_contact_support()