import pymysql
import psycopg2
from config import MYSQL_HOST, MYSQL_NAME, MYSQL_PASS, MYSQL_USER, PG_HOST, PG_PASS, PG_NAME, PG_USER, PG_PORT
import datetime
import time
import ipaddress

def tr_messages(amount):
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
            my_cursor.execute("SELECT * FROM `messages` ORDER BY id DESC")
            rows = my_cursor.fetchmany(amount)
            st = time.time()
            string = ''

            type_request = True
            #while rows:
            while rows:
                for row in rows:
                    message_id = row[0]
                    # insert data into table postgres
                    from_user_id = row[1]
                    # if from_user_id in deletion_uid:
                    #     continue
                    to_user_id = row[2]
                    # if to_user_id in deletion_uid:
                    #     continue
                    message = row[3].replace("'", "''")
                    uploads_id_id = row[4]
                    if not uploads_id_id:
                        uploads_id_id = 'NULL'
                    else:
                        message = "📎"
                    sent = row[5]
                    read = row[6]
                    # if not read:
                    #    read = False
                    date_time = row[8]
                    link = row[10]
                    spam = row[11]
                    chat = row[7]
                    support = False
                    
                    string += f"""(
                            '{message_id}',
                            '{message}',
                            '{to_user_id}',
                            '{from_user_id}',
                            {uploads_id_id},
                            '{sent}',
                            '{read}',
                            '{date_time}',
                            '{link}',
                            '{spam}',
                            '{chat}',
                            '{support}'
                        ),"""
                string = string[:len(string) - 1]
                
                try:
                    if len(string) > 3:
                        if type_request:
                            pg_cursor.execute(
                                f"""BEGIN;
                                    SET CONSTRAINTS ALL DEFERRED;
                                    INSERT INTO mcams_messages (
                                    id,
                                    message,
                                    to_user_id,
                                    from_user_id,
                                    uploads_id_id,
                                    sent,
                                    read,
                                    date_time,
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
                            
                            # type_request = False
                        else:
                            pg_cursor.execute(
                                    f"""BEGIN;
                                        SET CONSTRAINTS ALL DEFERRED;
                                        INSERT INTO mcams_messages (
                                        id,
                                        message,
                                        to_user_id,
                                        from_user_id,
                                        uploads_id_id,
                                        sent,
                                        read,
                                        date_time,
                                        link,
                                        spam,
                                        chat,
                                        support
                                        ) VALUES {string} ON CONFLICT (id) DO NOTHING;
                                        DELETE FROM mcams_messages
                                        WHERE mcams_messages.uploads_id_id IS NOT NULL AND NOT EXISTS (
                                        SELECT 1
                                        FROM mcams_file
                                        WHERE mcams_file.id = mcams_messages.uploads_id_id
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