import pymysql
import psycopg2
from config import MYSQL_HOST, MYSQL_NAME, MYSQL_PASS, MYSQL_USER, PG_HOST, PG_PASS, PG_NAME, PG_USER, PG_PORT
import datetime
import time
import ipaddress

def tr_contact_tickets(amount):
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
            my_cursor.execute("SELECT * FROM `contact_tickets`")
            rows = my_cursor.fetchmany(amount)
            st = time.time()
            string = ''
            
            #while rows:
            while rows:
                for row in rows:
                    # insert data into table postgres
                    uid = 'support_uid'
                    contact_uid = row[1]
                    msg_id = row[3]
                    if int(row[5]) == 0:
                        ticket = True
                    else:
                        ticket = False
                    last_msg = row[2].replace("'", "''")
                    if row[4] != 0:
                        last_msg = '📎'
                    
                    string += f"""(
                            '{uid}',
                            '{contact_uid}',
                            '{msg_id}',
                            '{last_msg}',
                             {0},
                            '{ticket}'
                        ),"""
                string = string[:len(string) - 1]
                try:
                    pg_cursor.execute(
                        f"""BEGIN;
                            SET CONSTRAINTS ALL DEFERRED;
                            INSERT INTO mcams_contacts (
                            uid_id,
                            contact_uid_id,
                            msg_id_id,
                            last_msg,
                            not_seen,
                            ticket
                            ) VALUES {string};

                            DELETE FROM mcams_contacts
                                WHERE NOT EXISTS (
                                SELECT 1
                                FROM mcams_user
                                WHERE mcams_user.uid = mcams_contacts.uid_id 
                                ) OR NOT EXISTS (
                                SELECT 1
                                FROM mcams_user
                                WHERE mcams_user.uid = mcams_contacts.contact_uid_id 
                                );

                            UPDATE mcams_contacts
                            SET msg_id_id = ( SELECT id
                                FROM mcams_messages WHERE 
                                    (mcams_messages.from_user_id = mcams_contacts.uid_id 
                                        AND mcams_messages.to_user_id = mcams_contacts.contact_uid_id) 
                                    OR (mcams_messages.to_user_id = mcams_contacts.uid_id 
                                        AND mcams_messages.from_user_id = mcams_contacts.contact_uid_id) 
                                ORDER BY date_time DESC LIMIT 1) 
                            WHERE uid_id = 'support_uid';
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
    tr_contact_tickets()