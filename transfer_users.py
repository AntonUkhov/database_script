import pymysql
import psycopg2
from config import MYSQL_HOST, MYSQL_NAME, MYSQL_PASS, MYSQL_USER, PG_HOST, PG_PASS, PG_NAME, PG_USER, PG_PORT
import datetime
import time
import ipaddress

def tr_users(amount):
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
            my_cursor.execute("SELECT * FROM `users`")
            rows = my_cursor.fetchmany(amount)
            st = time.time()
            string = ''
            
            #while rows:
            while rows:
                make_ban = ''
                for digit in range(len(rows)):
                    make_ban += "(" + 'false' + ',' + 'false' + ',' + 'false' + ',' + 'false' + ',' + 'false' + ',' + 'false' + ',' + 'false' + ',' + 'false' + "),"
                make_ban = make_ban[:len(make_ban)-1]
                pg_cursor.execute(f"""INSERT INTO mcams_baduser (
                                    call,
                                    message,
                                    download,
                                    firebase,
                                    profile_image,
                                    complain,
                                    change_name,
                                    virtual_device
                ) VALUES {make_ban} RETURNING id""")
                ban_ids = pg_cursor.fetchall()
                
                index = 0

                for row in rows:
                    uid = row[1]
                    username = row[2].replace("'", "''")
                    email = row[3]
                    emailVerified = row[4]
                    if not emailVerified:
                        emailVerified = False
                    isAnonymous = row[5]
                    if not isAnonymous:
                        isAnonymous = False
                    gender = row[6]
                    age = row[7]
                    if not age:
                        age = 0
                    sex = row[8]
                    country = row[9]
                    about = row[10].replace("'", "''")
                    #if about and "'" in about:
                    #    print(about)
                    receive_calls = row[13]
                    lang = country # row[16]
                    call_sound = row[11]
                    if not call_sound:
                        call_sound = False
                    notify_sound = row[12]
                    if not notify_sound:
                        notify_sound = False
                    about_approved = row[14]
                    if not about_approved:
                        about_approved = False
                    account_approved = row[15]
                    if not account_approved:
                        account_approved = False
                    last_login = row[18]
                    date_age = datetime.date.today()
                    ip = row[19]
                    if ip:
                        ip = ipaddress.ip_address(row[19]).__str__()
                    date_registration = row[17]
                    fingerprint = row[22]
                    recaptcha = row[23]
                    if not recaptcha:
                        recaptcha = 0.9
                    support = False
                    receive_messages = 1
                    bad_words = False
                    spam_score = 0
                    deletion_status = False
                    bot = False
                    screen_notifications = False
                    ban = ban_ids[index][0]
                    useragent = row[21]
                    index += 1
                    string += f"""(
                        '{uid}',
                        '{username}',
                        '{email}',
                        '{emailVerified}',
                        '{isAnonymous}',
                        '{gender}',
                        '{age}',
                        '{sex}',
                        '{country}',
                        '{about}',
                        '{receive_calls}',
                        '{lang}',
                        '{call_sound}',
                        '{notify_sound}',
                        '{about_approved}',
                        '{account_approved}',
                        '{last_login}',
                        '{ip}',
                        '{date_age}',
                        '{date_registration}',
                        '{fingerprint}',
                        '{recaptcha}',
                        '{receive_messages}',
                        '{bad_words}',
                        '{spam_score}',
                        '{deletion_status}',
                        '{bot}',
                        '{screen_notifications}',
                        '{ban}',
                        '{useragent}'
                        ),"""
                string = string[:len(string) - 1]
                try:
                    pg_cursor.execute(
                        f"""INSERT INTO mcams_user (
                            uid,
                            username,
                            email,
                            "emailVerified",
                            "isAnonymous",
                            gender,
                            age,
                            sex,
                            country,
                            about,
                            receive_calls,
                            lang,
                            call_sound,
                            notify_sound,
                            about_approved,
                            account_approved,
                            last_login,
                            ip,
                            date_age,
                            date_registration,
                            fingerprint,
                            recaptcha,
                            receive_messages,
                            bad_words,
                            spam_score,
                            deletion_status,
                            bot,
                            screen_notifications,
                            ban_id,
                            useragent
                            ) VALUES {string}""")
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