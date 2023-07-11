import pymysql
import psycopg2
from config import MYSQL_HOST, MYSQL_NAME, MYSQL_PASS, MYSQL_USER, PG_HOST, PG_PASS, PG_NAME, PG_USER, PG_PORT
import datetime
import time
import ipaddress

def tr_users_deleted(amount):
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
            my_cursor.execute("SELECT * FROM `users_deleted`")
            rows = my_cursor.fetchmany(amount)
            string = ''
            
            while rows:
                uid_list = []
                for row in rows:
                    uid = row[1]
                    reason_for_deletion = row[4].replace("'", "''")
                    email = row[2]
                    deletion_status = row[7]
                    if deletion_status:
                        deletion_status = True
                        uid_list.append("'" + uid + "'")
                    else:
                        deletion_status = False
                    reason_for_deletion_type = row[3]
                    if reason_for_deletion_type is None:
                        reason_for_deletion_type = 0
                    gender = row[5]
                    if gender is None:
                        gender = 2
                    account_approved = bool(row[6])
                    deletion_time = row[8]
                    if deletion_time == "0000-00-00 00:00:00":
                        deletion_time = datetime.datetime.now()
                    
                    string += f"""(
                            '{reason_for_deletion}',
                            '{deletion_status}',
                            '{uid}',
                            '{reason_for_deletion_type}',
                            '{gender}',
                            '{account_approved}',
                            '{deletion_time}',
                            '{email}'
                        ),"""
                string = string[:len(string) - 1]
                try:
                    deletion_status_ = True
                    
                    pg_cursor.execute(
                        f"""INSERT INTO mcams_deletedusers (
                            reason_for_deletion,
                            deletion_status,
                            uid,
                            reason_for_deletion_type,
                            gender,
                            account_approved,
                            deletion_time,
                            email
                            ) VALUES {string} ON CONFLICT (uid) DO NOTHING""")
                    deletion_status_ = True
                    pg_cursor.execute(
                        f"""UPDATE mcams_user SET deletion_status = {deletion_status_} WHERE uid IN ({', '.join(uid_list)})""")
                        
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
    tr_users_deleted(10000)