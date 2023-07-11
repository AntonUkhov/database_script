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
            # bad_fraud
            my_cursor.execute("SELECT * FROM `bad_fraud`")
            row = my_cursor.fetchone()
            a = my_cursor.fetchmany
            while row is not None:
                # insert data into table postgres
                value = row[1]
                pg_cursor.execute(f"INSERT INTO mcams_badfraud (bad_fraud) VALUES ('{value}')")
                row = my_cursor.fetchone()

            # bad_fraud_words
            my_cursor.execute("SELECT * FROM `bad_fraud_words`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                value = row[1]
                pg_cursor.execute(f"INSERT INTO mcams_badfraudwords (bad_fraud_words) VALUES ('{value}')")
                row = my_cursor.fetchone()
            
            # bad_spam
            my_cursor.execute("SELECT * FROM `bad_spam`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                value = row[1]
                pg_cursor.execute(f"INSERT INTO mcams_badspam (bad_spam) VALUES ('{value}')")
                row = my_cursor.fetchone()
            
            # bad_words
            my_cursor.execute("SELECT * FROM `bad_words`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                value = row[1]
                pg_cursor.execute(f"INSERT INTO mcams_badwords (bad_words) VALUES ('{value}')")
                row = my_cursor.fetchone()

            # banned_ip
            # my_cursor.execute("SELECT * FROM `banned_ip`")
            # row = my_cursor.fetchone()
            # while row is not None:
            #     # insert data into table postgres
            #     ip = row[1]
            #     message = False
            #     if row[2] == 1:
            #         message = True
            #     download = False
            #     if row[3] == 1:
            #         download = True
            #     call = False
            #     if row[4] == 1:
            #         call = True
            #     pg_cursor.execute(
            #         f"INSERT INTO mcams_banip (ip, call, message, download) VALUES ('{ip}', '{call}', '{message}', '{download}')")
            #     row = my_cursor.fetchone()

            # users
            my_cursor.execute("SELECT * FROM `users`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                uid = row[1]
                username = row[2]
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
                about = row[10]
                receive_calls = row[13]
                lang = row[16]
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
                ip = row[19]
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
                reason_for_deletion_type = 0
                bot = False
                screen_notifications = False
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
                        date_registration,
                        fingerprint,
                        recaptcha,
                        support,
                        receive_messages,
                        bad_words,
                        spam_score,
                        deletion_status,
                        reason_for_deletion_type,
                        bot,
                        screen_notifications
                        ) VALUES (
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
                        '{date_registration}',
                        '{fingerprint}',
                        '{recaptcha}',
                        '{support}',
                        '{receive_messages}',
                        '{bad_words}',
                        '{spam_score}',
                        '{deletion_status}',
                        '{reason_for_deletion_type}',
                        '{bot}',
                        '{screen_notifications}'
                        )""")
                row = my_cursor.fetchone()

            # black_list
            my_cursor.execute("SELECT * FROM `black_list`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                uid = row[1]
                contact_uid = row[2]
                try:
                    pg_cursor.execute(
                        f"INSERT INTO mcams_blacklist (uid_id, contact_uid_id) VALUES ('{uid}', '{contact_uid}')")
                except Exception as ex:
                    print(ex)
                row = my_cursor.fetchone()

            # favorite_list
            my_cursor.execute("SELECT * FROM `favorite_list`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                uid = row[1]
                contact_uid = row[2]
                try:
                    pg_cursor.execute(
                        f"INSERT INTO mcams_favoritelist (uid_id, contact_uid_id) VALUES ('{uid}', '{contact_uid}')")
                except Exception as ex:
                    print(ex)
                row = my_cursor.fetchone()

            # file
            my_cursor.execute("SELECT * FROM `user_uploads`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                owner_id = row[1]
                row_for_old_db = f'uploads/{owner_id}/'
                small_file = row_for_old_db + row[4]
                middle_file = row_for_old_db + row[3]
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

            # messages
            my_cursor.execute("SELECT * FROM `messages`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                from_user_id = row[1]
                to_user_id = row[2]
                message = row[3]
                message = message.replace("'", "''")
                uploads_id_id = row[4]
                if not uploads_id_id:
                    uploads_id_id = 'NULL'
                sent = row[5]
                read = row[6]
                # if not read:
                #    read = False
                date_time = row[8]
                link = row[10]
                spam = row[11]
                chat = row[7]
                support = False
                try:
                    pg_cursor.execute(
                        f"""INSERT INTO mcams_messages (
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
                                    ) VALUES (
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
                                    )""")
                except Exception as ex:
                    print(ex)
                row = my_cursor.fetchone()

            # # logs
            # my_cursor.execute("SELECT * FROM `logs`")
            # row = my_cursor.fetchone()
            # while row is not None:
            #     # insert data into table postgres
            #     user_uid = row[1]
            #     log = row[2]
            #     type = row[3]
            #     if 'Link' in type:
            #         type = 3
            #     elif 'error' in type:
            #         type = 1
            #     elif 'ban' in type:
            #         type = 2
            #     elif 'claim' in type:
            #         type = 4
            #     elif 'info' in type:
            #         type = 5
            #     elif 'alert' in type:
            #         type = 6
            #     date = row[4]
            #     user_id = ''
            #     read = False
            #     try:
            #         pg_cursor.execute(
            #             f"""INSERT INTO mcams_logs (
            #                  user_uid,
            #                  log,
            #                  type,
            #                  date,
            #                  read
            #                  ) VALUES (
            #                  '{user_uid}',
            #                  '{log}',
            #                  '{type}',
            #                  '{date}',
            #                  '{read}'
            #                  )""")
            #     except Exception as ex:
            #         print(ex)
            #     row = my_cursor.fetchone()

            # contacts
            my_cursor.execute("SELECT * FROM `contacts`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                uid_id = row[1]
                contact_uid_id = row[2]
                msg_id_id = row[3]
                last_msg = row[4]
                not_seen = row[6]
                ticket = False
                try:
                    pg_cursor.execute(
                        f"""INSERT INTO mcams_contacts (
                                     uid_id,
                                     contact_uid_id,
                                     msg_id_id,
                                     last_msg,
                                     not_seen,
                                     ticket
                                     ) VALUES (
                                     '{uid_id}',
                                     '{contact_uid_id}',
                                     '{msg_id_id}',
                                     '{last_msg}',
                                     '{not_seen}',
                                     '{ticket}'
                                     )""")
                except Exception as ex:
                    print(ex)
                row = my_cursor.fetchone()

            # Complaint
            my_cursor.execute("SELECT * FROM `claims`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                from_uid_id = row[1]
                to_uid_id = row[2]
                complain = row[3]
                if complain == 0:
                    complain = 'obscene'
                elif complain == 1:
                    complain = 'spam'
                elif complain == 2:
                    complain = 'gender'
                elif complain == 3:
                    complain = 'csam'
                elif complain == 4:
                    complain = 'name'
                elif complain == 5:
                    complain = 'underage'
                date_time = row[4]
                read = False
                try:
                    pg_cursor.execute(
                        f"""INSERT INTO mcams_complaint (
                                    from_uid_id,
                                    to_uid_id,
                                    complain,
                                    date_time,
                                    read
                                    ) VALUES (
                                    '{from_uid_id}',
                                    '{to_uid_id}',
                                    '{complain}',
                                    '{date_time}',
                                    '{read}'
                                    )""")
                except Exception as ex:
                    print(ex)
                row = my_cursor.fetchone()

            # banned_hash
            my_cursor.execute("SELECT * FROM `banned_hash`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                bad_hash = row[1]
                uid = row[2]
                date_time = row[3]
                try:
                    pg_cursor.execute(
                        f"""INSERT INTO mcams_badhash (
                                    bad_hash,
                                    uid,
                                    date_time
                                    ) VALUES (
                                    '{bad_hash}',
                                    '{uid}',
                                    '{date_time}'
                                    )""")
                except Exception as ex:
                    print(ex)
                row = my_cursor.fetchone()

            # banned_hash_fraud
            my_cursor.execute("SELECT * FROM `banned_hash_fraud`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                hash = row[1]
                date_time = datetime.datetime.now()
                uid = ''
                try:
                    pg_cursor.execute(
                        f"""INSERT INTO mcams_badhash (
                                            bad_hash,
                                            date_time,
                                            uid
                                            ) VALUES (
                                            '{hash}',
                                            '{date_time}',
                                            '{uid}'
                                            )""")
                except Exception as ex:
                    print(ex)
                row = my_cursor.fetchone()

            # bene_roles
            my_cursor.execute("SELECT * FROM `bene_roles`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                uid = row[1]
                bene_roles = row[2]
                if bene_roles == 3:
                    bene_roles = 4
                elif bene_roles == 4:
                    bene_roles = 3
                try:
                    pg_cursor.execute(
                        f"""UPDATE mcams_user SET bene_roles = {bene_roles} WHERE uid = '{uid}'""")
                except Exception as ex:
                    print(ex)
                row = my_cursor.fetchone()

            # users_deleted
            my_cursor.execute("SELECT * FROM `users_deleted`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                uid = row[1]
                reason_for_deletion = row[4]
                deletion_status = row[7]
                if deletion_status:
                    deletion_status = True
                else:
                    deletion_status = False
                try:
                    pg_cursor.execute(
                        f"""UPDATE mcams_user SET reason_for_deletion = '{reason_for_deletion}', deletion_status = {deletion_status} WHERE uid = '{uid}'""")
                except Exception as ex:
                    print(ex)
                row = my_cursor.fetchone()

            # # banned_uid
            # my_cursor.execute("SELECT * FROM `banned_uid`")
            # row = my_cursor.fetchone()
            # while row is not None:
            #     # insert data into table postgres
            #     uid = row[6]
            #     call = row[4]
            #     if not call:
            #         call = False
            #     else:
            #         call = True
            #     message = row[1]
            #     if not message:
            #         message = False
            #     else:
            #         message = True
            #     change_name = row[3]
            #     if not change_name:
            #         change_name = False
            #     else:
            #         change_name = True
            #     download = row[2]
            #     if not download:
            #         download = False
            #     else:
            #         download = True
            #     firebase = False
            #     profile_image = False
            #     complain = False
            #     ip_message = False
            #     ip_network_message = False
            #     ip_download = False
            #     ip_network_download = False
            #     ip_call = False
            #     ip_network_call = False
            #     try:
            #         pg_cursor.execute(
            #             f"""SELECT * FROM mcams_user WHERE uid = '{uid}'""")
            #         pg_row = pg_cursor.fetchone()
            #         if pg_row[-11] is None:
            #             pg_cursor.execute(
            #                 f"""INSERT INTO mcams_baduser (
            #                                          call,
            #                                          message,
            #                                          download,
            #                                          change_name,
            #                                          firebase,
            #                                          profile_image,
            #                                          complain,
            #                                          ip_message,
            #                                          ip_network_message,
            #                                          ip_download,
            #                                          ip_network_download,
            #                                          ip_call,
            #                                          ip_network_call
            #                                          ) VALUES (
            #                                          '{call}',
            #                                          '{message}',
            #                                          '{download}',
            #                                          '{change_name}',
            #                                          '{firebase}',
            #                                          '{profile_image}',
            #                                          '{complain}',
            #                                          '{ip_message}',
            #                                          '{ip_network_message}',
            #                                          '{ip_download}',
            #                                          '{ip_network_download}',
            #                                          '{ip_call}',
            #                                          '{ip_network_call}'
            #                                          )""")
            #             pg_cursor.execute(
            #                 f"""SELECT MAX(id) FROM mcams_baduser"""
            #             )
            #             ban_id = pg_cursor.fetchone()[0]
            #             pg_cursor.execute(
            #                 f"""UPDATE mcams_user SET ban_id = {ban_id} WHERE uid = '{uid}'""")
            #         else:
            #             pg_cursor.execute(
            #                 f"""UPDATE mcams_baduser SET call = {call}, message = {message}, download = {download}, change_name = {change_name} WHERE id = '{pg_row[-11]}'""")
            #     except Exception as ex:
            #         print(ex)
            #     row = my_cursor.fetchone()


            my_cursor.execute("SELECT * FROM `bad_devices`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                bad_device = row[1]
                try:
                    pg_cursor.execute(
                        f"""INSERT INTO mcams_baddevice (
                                    bad_device
                                    ) VALUES (
                                    '{bad_device}'
                                    )""")
                except Exception as ex:
                    print(ex)
                row = my_cursor.fetchone()

            
            # my_cursor.execute("SELECT * FROM `bad_fingerprint`")
            # row = my_cursor.fetchone()
            # while row is not None:
            #     # insert data into table postgres
            #     bad_fingerprint = row[1]
            #     try:
            #         pg_cursor.execute(
            #             f"""INSERT INTO mcams_badfingerprint (
            #                         bad_fingerprint
            #                         ) VALUES (
            #                         '{bad_fingerprint}'
            #                         )""")
            #     except Exception as ex:
            #         print(ex)
            #     row = my_cursor.fetchone()


            my_cursor.execute("SELECT * FROM `contact_support`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                if int(row[2]) == 0:
                    from_user = row[1]
                    to_user = 'support_uid'
                    support_name = row[3]
                else:
                    from_user = 'support_uid'
                    to_user = row[1]
                    support_name = row[1]
                message = row[4]
                uploads_id = row[5]
                read = row[8]
                date_time = row[6]
                sent = row[7]
                link = 0
                spam = 0
                chat = True
                support = True
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
                                    ) VALUES (
                                    '{from_user}',
                                    '{to_user}',
                                    '{support_name}',
                                    '{message}',
                                    '{uploads_id}',
                                    '{read}',
                                    '{date_time}',
                                    '{sent}',
                                    '{link}',
                                    '{spam}',
                                    '{chat}',
                                    '{support}'
                                    )""")
                except Exception as ex:
                    print(ex)
                row = my_cursor.fetchone()


            my_cursor.execute("SELECT * FROM `contact_tickets`")
            row = my_cursor.fetchone()
            while row is not None:
                # insert data into table postgres
                uid = 'support_uid'
                contact_uid = row[1]
                msg_id = row[3]
                if int(row[5]) == 1:
                    ticket = True
                else:
                    ticket = False
                last_msg = row[2]
                if row[4] is not None:
                    last_msg = '📎'
                try:
                    pg_cursor.execute(
                        f"""INSERT INTO mcams_contacts (
                                    uid_id,
                                    contact_uid_id,
                                    msg_id_id,
                                    last_msg,
                                    ticket
                                    ) VALUES (
                                    '{uid}',
                                    '{contact_uid}',
                                    '{msg_id}',
                                    '{last_msg}',
                                    '{ticket}'
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
