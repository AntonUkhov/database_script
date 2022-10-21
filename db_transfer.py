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
        with mysql_conn.cursor() as my_cursor, pg_conn.cursor() as pg_cursor:
            ## bad_fraud
            #my_cursor.execute("SELECT * FROM `bad_fraud`")
            #row = my_cursor.fetchone()
            #while row is not None:
            #    # insert data into table postgres
            #    value = row[1]
            #    pg_cursor.execute(f"INSERT INTO mcams_badfraud (bad_fraud) VALUES ('{value}')")
            #    row = my_cursor.fetchone()
#
            ## bad_fraud_words
            #my_cursor.execute("SELECT * FROM `bad_fraud_words`")
            #row = my_cursor.fetchone()
            #while row is not None:
            #    # insert data into table postgres
            #    value = row[1]
            #    pg_cursor.execute(f"INSERT INTO mcams_badfraudwords (bad_fraud_words) VALUES ('{value}')")
            #    row = my_cursor.fetchone()
##
            ## bad_spam
            #my_cursor.execute("SELECT * FROM `bad_spam`")
            #row = my_cursor.fetchone()
            #while row is not None:
            #    # insert data into table postgres
            #    value = row[1]
            #    pg_cursor.execute(f"INSERT INTO mcams_badspam (bad_spam) VALUES ('{value}')")
            #    row = my_cursor.fetchone()
##
            ## bad_words
            #my_cursor.execute("SELECT * FROM `bad_words`")
            #row = my_cursor.fetchone()
            #while row is not None:
            #    # insert data into table postgres
            #    value = row[1]
            #    pg_cursor.execute(f"INSERT INTO mcams_badwords (bad_words) VALUES ('{value}')")
            #    row = my_cursor.fetchone()
#
            ## banned_ip
            #my_cursor.execute("SELECT * FROM `banned_ip`")
            #row = my_cursor.fetchone()
            #while row is not None:
            #    # insert data into table postgres
            #    ip = row[1]
            #    message = False
            #    if row[2] == 1:
            #        message = True
            #    download = False
            #    if row[3] == 1:
            #        download = True
            #    call = False
            #    if row[4] == 1:
            #        call = True
            #    pg_cursor.execute(f"INSERT INTO mcams_banip (ip, call, message, download) VALUES ('{ip}', '{call}', '{message}', '{download}')")
            #    row = my_cursor.fetchone()

            ## users
            #my_cursor.execute("SELECT * FROM `users`")
            #row = my_cursor.fetchone()
            #while row is not None:
            #    # insert data into table postgres
            #    uid = row[1]
            #    username = row[2]
            #    email = row[3]
            #    emailVerified = row[4]
            #    if not emailVerified:
            #        emailVerified = False
            #    isAnonymous = row[5]
            #    if not isAnonymous:
            #        isAnonymous = False
            #    gender = row[6]
            #    age = row[7]
            #    if not age:
            #        age = 0
            #    sex = row[8]
            #    country = row[9]
            #    about = row[10]
            #    receive_calls = row[13]
            #    lang = row[16]
            #    call_sound = row[11]
            #    if not call_sound:
            #        call_sound = False
            #    notify_sound = row[12]
            #    if not notify_sound:
            #        notify_sound = False
            #    about_approved = row[14]
            #    if not about_approved:
            #        about_approved = False
            #    account_approved = row[15]
            #    if not account_approved:
            #        account_approved = False
            #    last_login = row[18]
            #    ip = row[19]
            #    date_registration = row[17]
            #    fingerprint = row[22]
            #    recaptcha = row[23]
            #    if not recaptcha:
            #        recaptcha = 0.9
            #    support = False
            #    receive_messages = 1
            #    bad_words = False
            #    spam_score = 0
            #    deletion_status = False
            #    pg_cursor.execute(
            #        f"""INSERT INTO mcams_user (
            #            uid,
            #            username,
            #            email,
            #            "emailVerified",
            #            "isAnonymous",
            #            gender,
            #            age,
            #            sex,
            #            country,
            #            about,
            #            receive_calls,
            #            lang,
            #            call_sound,
            #            notify_sound,
            #            about_approved,
            #            account_approved,
            #            last_login,
            #            ip,
            #            date_registration,
            #            fingerprint,
            #            recaptcha,
            #            support,
            #            receive_messages,
            #            bad_words,
            #            spam_score,
            #            deletion_status
            #            ) VALUES (
            #            '{uid}',
            #            '{username}',
            #            '{email}',
            #            '{emailVerified}',
            #            '{isAnonymous}',
            #            '{gender}',
            #            '{age}',
            #            '{sex}',
            #            '{country}',
            #            '{about}',
            #            '{receive_calls}',
            #            '{lang}',
            #            '{call_sound}',
            #            '{notify_sound}',
            #            '{about_approved}',
            #            '{account_approved}',
            #            '{last_login}',
            #            '{ip}',
            #            '{date_registration}',
            #            '{fingerprint}',
            #            '{recaptcha}',
            #            '{support}',
            #            '{receive_messages}',
            #            '{bad_words}',
            #            '{spam_score}',
            #            '{deletion_status}'
            #            )""")
            #    row = my_cursor.fetchone()

            ## black_list
            #my_cursor.execute("SELECT * FROM `black_list`")
            #row = my_cursor.fetchone()
            #while row is not None:
            #    # insert data into table postgres
            #    uid = row[1]
            #    contact_uid = row[2]
            #    try:
            #        pg_cursor.execute(f"INSERT INTO mcams_blacklist (uid_id, contact_uid_id) VALUES ('{uid}', '{contact_uid}')")
            #    except Exception as ex:
            #        print(ex)
            #    row = my_cursor.fetchone()

            ## favorite_list
            #my_cursor.execute("SELECT * FROM `favorite_list`")
            #row = my_cursor.fetchone()
            #while row is not None:
            #    # insert data into table postgres
            #    uid = row[1]
            #    contact_uid = row[2]
            #    try:
            #        pg_cursor.execute(
            #            f"INSERT INTO mcams_favoritelist (uid_id, contact_uid_id) VALUES ('{uid}', '{contact_uid}')")
            #    except Exception as ex:
            #        print(ex)
            #    row = my_cursor.fetchone()

            ## file
            #my_cursor.execute("SELECT * FROM `user_uploads`")
            #row = my_cursor.fetchone()
            #while row is not None:
            #    # insert data into table postgres
            #    owner_id = row[1]
            #    small_file = row[4]
            #    middle_file = row[3]
            #    large_file = row[2]
            #    datetime = row[12]
            #    size = row[10]
            #    size = size / 1024
            #    type = row[8]
            #    if type == 'image':
            #        type = 1
            #    else:
            #        type = 2
            #    check_file = 1 # row[5]
            #    check_profile_image = row[6]
            #    if not check_profile_image:
            #        check_profile_image = False
            #    hash = row[9]
            #    file_link = ''
            #    try:
            #        pg_cursor.execute(
            #            f"""INSERT INTO mcams_file (
            #                check_file,
            #                owner_id,
            #                small_file,
            #                middle_file,
            #                large_file,
            #                datetime,
            #                size,
            #                type,
            #                check_profile_image,
            #                hash,
            #                file_link
            #                ) VALUES (
            #                '{check_file}',
            #                '{owner_id}',
            #                '{small_file}',
            #                '{middle_file}',
            #                '{large_file}',
            #                '{datetime}',
            #                '{size}',
            #                '{type}',
            #                '{check_profile_image}',
            #                '{hash}',
            #                '{file_link}'
            #                )""")
            #    except Exception as ex:
            #        print(ex)
            #    row = my_cursor.fetchone()

            ## messages
            #my_cursor.execute("SELECT * FROM `messages`")
            #row = my_cursor.fetchone()
            #while row is not None:
            #    # insert data into table postgres
            #    from_user_id = row[1]
            #    to_user_id = row[2]
            #    message = row[3]
            #    message = message.replace("'", "''")
            #    uploads_id_id = row[4]
            #    if not uploads_id_id:
            #        uploads_id_id = 'NULL'
            #    sent = row[5]
            #    read = row[6]
            #    #if not read:
            #    #    read = False
            #    date_time = row[8]
            #    link = row[10]
            #    spam = row[11]
            #    chat = row[7]
            #    support = False
            #    try:
            #        pg_cursor.execute(
            #            f"""INSERT INTO mcams_messages (
            #                        message,
            #                        to_user_id,
            #                        from_user_id,
            #                        uploads_id_id,
            #                        sent,
            #                        read,
            #                        date_time,
            #                        link,
            #                        spam,
            #                        chat,
            #                        support
            #                        ) VALUES (
            #                        '{message}',
            #                        '{to_user_id}',
            #                        '{from_user_id}',
            #                        {uploads_id_id},
            #                        '{sent}',
            #                        '{read}',
            #                        '{date_time}',
            #                        '{link}',
            #                        '{spam}',
            #                        '{chat}',
            #                        '{support}'
            #                        )""")
            #    except Exception as ex:
            #        print(ex)
            #    row = my_cursor.fetchone()

    finally:
        if mysql_conn:
            mysql_conn.close()
        if pg_conn:
            pg_conn.close()
except Exception as ex:
    print('Connection refused...')
    print(ex)

