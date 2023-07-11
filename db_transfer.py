import pymysql
import psycopg2
from config import MYSQL_HOST, MYSQL_NAME, MYSQL_PASS, MYSQL_USER, PG_HOST, PG_PASS, PG_NAME, PG_USER, PG_PORT
import datetime
import time
import ipaddress


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
        # # select all data from table mysql
        # with mysql_conn.cursor() as my_cursor, pg_conn.cursor() as pg_cursor:
        #     # bad_fraud
        #     my_cursor.execute("SELECT * FROM `bad_fraud`")
        #     rows = my_cursor.fetchmany(1000)
        #     string = ''
        #     while rows:
        #         for row in rows:
        #             string += "('" + row[1] + "'),"
        #         string = string[:len(string)-1]
        #         pg_cursor.execute(f"INSERT INTO mcams_badfraud (bad_fraud) VALUES {string}")
        #         rows = my_cursor.fetchmany(1000)
        #         string = ''


            # # bad_fraud_words
            
            # my_cursor.execute("SELECT * FROM `bad_fraud_words`")
            # rows = my_cursor.fetchmany(500)
            # string = ''
            # while rows:
            #     for row in rows:
            #         string += "('" + row[1] + "'),"
            #     string = string[:len(string)-1]
            #     pg_cursor.execute(f"INSERT INTO mcams_badfraudwords (bad_fraud_words) VALUES {string}")
            #     rows = my_cursor.fetchmany(500)
            #     string = ''
            

            
            # # bad_spam
            
            # my_cursor.execute("SELECT * FROM `bad_spam`")
            # rows = my_cursor.fetchmany(500)
            # string = ''
            # while rows:
            #     # insert data into table postgres
            #     for row in rows:
            #         string += "('" + row[1] + "'),"
            #     string = string[:len(string)-1]
            #     pg_cursor.execute(f"INSERT INTO mcams_badspam (bad_spam) VALUES {string}")
            #     rows = my_cursor.fetchmany(500)
            #     string = ''

            
            # # bad_words
            # st = time.time()
            # my_cursor.execute("SELECT * FROM `bad_words`")
            # rows = my_cursor.fetchmany(500)
            # string = ''
            # while rows:
            #     # insert data into table postgres
            #     for row in rows:
            #         string += "('" + row[1] + "'),"
            #     string = string[:len(string)-1]
            #     pg_cursor.execute(f"INSERT INTO mcams_badwords (bad_words) VALUES {string}")
            #     rows = my_cursor.fetchmany(500)
            #     string = ''
            # print(time.time() - st)




            # # users
            # my_cursor.execute("SELECT * FROM `users`")
            # rows = my_cursor.fetchmany(10000)
            # string = ''
            # count = 3000
            # #while rows:
            # while rows:
            #     make_ban = ''
            #     for digit in range(len(rows)):
            #         make_ban += "(" + 'false' + ',' + 'false' + ',' + 'false' + ',' + 'false' + ',' + 'false' + ',' + 'false' + ',' + 'false' + ',' + 'false' + "),"
            #     make_ban = make_ban[:len(make_ban)-1]
            #     pg_cursor.execute(f"""INSERT INTO mcams_baduser (
            #                         call,
            #                         message,
            #                         download,
            #                         firebase,
            #                         profile_image,
            #                         complain,
            #                         change_name,
            #                         virtual_device
            #     ) VALUES {make_ban} RETURNING id""")
            #     ban_ids = pg_cursor.fetchall()
                
            #     count += len(rows)
            #     index = 0

            #     for row in rows:
            #         uid = row[1]
            #         username = row[2].replace("'", "''")
            #         email = row[3]
            #         emailVerified = row[4]
            #         if not emailVerified:
            #             emailVerified = False
            #         isAnonymous = row[5]
            #         if not isAnonymous:
            #             isAnonymous = False
            #         gender = row[6]
            #         age = row[7]
            #         if not age:
            #             age = 0
            #         sex = row[8]
            #         country = row[9]
            #         about = row[10].replace("'", "''")
            #         if about and "'" in about:
            #             print(about)
            #         receive_calls = row[13]
            #         lang = country # row[16]
            #         call_sound = row[11]
            #         if not call_sound:
            #             call_sound = False
            #         notify_sound = row[12]
            #         if not notify_sound:
            #             notify_sound = False
            #         about_approved = row[14]
            #         if not about_approved:
            #             about_approved = False
            #         account_approved = row[15]
            #         if not account_approved:
            #             account_approved = False
            #         last_login = row[18]
            #         date_age = datetime.date.today()
            #         ip = row[19]
            #         if ip:
            #             ip = ipaddress.ip_address(row[19]).__str__()
            #         date_registration = row[17]
            #         fingerprint = row[22]
            #         recaptcha = row[23]
            #         if not recaptcha:
            #             recaptcha = 0.9
            #         support = False
            #         receive_messages = 1
            #         bad_words = False
            #         spam_score = 0
            #         deletion_status = False
            #         bot = False
            #         screen_notifications = False
            #         ban = ban_ids[index][0]
            #         useragent = row[21]
            #         index += 1
            #         string += f"""(
            #             '{uid}',
            #             '{username}',
            #             '{email}',
            #             '{emailVerified}',
            #             '{isAnonymous}',
            #             '{gender}',
            #             '{age}',
            #             '{sex}',
            #             '{country}',
            #             '{about}',
            #             '{receive_calls}',
            #             '{lang}',
            #             '{call_sound}',
            #             '{notify_sound}',
            #             '{about_approved}',
            #             '{account_approved}',
            #             '{last_login}',
            #             '{ip}',
            #             '{date_age}',
            #             '{date_registration}',
            #             '{fingerprint}',
            #             '{recaptcha}',
            #             '{receive_messages}',
            #             '{bad_words}',
            #             '{spam_score}',
            #             '{deletion_status}',
            #             '{bot}',
            #             '{screen_notifications}',
            #             '{ban}',
            #             '{useragent}'
            #             ),"""
            #     string = string[:len(string) - 1]
            #     pg_cursor.execute(
            #         f"""INSERT INTO mcams_user (
            #             uid,
            #             username,
            #             email,
            #             "emailVerified",
            #             "isAnonymous",
            #             gender,
            #             age,
            #             sex,
            #             country,
            #             about,
            #             receive_calls,
            #             lang,
            #             call_sound,
            #             notify_sound,
            #             about_approved,
            #             account_approved,
            #             last_login,
            #             ip,
            #             date_age,
            #             date_registration,
            #             fingerprint,
            #             recaptcha,
            #             receive_messages,
            #             bad_words,
            #             spam_score,
            #             deletion_status,
            #             bot,
            #             screen_notifications,
            #             ban_id,
            #             useragent
            #             ) VALUES {string}""")
            #     rows = my_cursor.fetchmany(10000)
            #     string = ''
            # print('1' * 50)


            # # black_list
            # st = time.time()
            # my_cursor.execute("""SELECT * FROM `black_list`""")
            # rows = my_cursor.fetchmany(10000)
            # string = ''
            # while rows:
            #     for row in rows:
            #         uid = row[1]
            #         contact_uid = row[2]
            #         string += f"""(
            #              '{uid}',
            #              '{contact_uid}'
            #             ),"""
            #     # insert data into table postgres
            
            #     string = string[:len(string) - 1]
            #     try:
            #         pg_cursor.execute(
            #             f"""BEGIN;
            #                 SET CONSTRAINTS ALL DEFERRED;
            #                 INSERT INTO mcams_blacklist (
            #                 uid_id, contact_uid_id
            #                 ) VALUES {string};
            #                 DELETE FROM mcams_blacklist
            #                 WHERE mcams_blacklist.uid_id IS NOT NULL AND NOT EXISTS (
            #                 SELECT 1
            #                 FROM mcams_user
            #                 WHERE mcams_user.uid = mcams_blacklist.uid_id
            #                 ) OR NOT EXISTS (
            #                 SELECT 1
            #                 FROM mcams_user
            #                 WHERE mcams_user.uid = mcams_blacklist.contact_uid_id 
            #                 );
            #                 COMMIT;""")
            #     except Exception as ex:
            #         print(ex)
            #     rows = my_cursor.fetchmany(10000)
            #     string = ''
            # print(time.time() - st)

            # # favorite_list
            # st = time.time()
            # my_cursor.execute("SELECT * FROM `favorite_list`")
            # rows = my_cursor.fetchmany(10000)
            # string = ''
            # while rows:
            #     for row in rows:
            #         uid = row[1]
            #         contact_uid = row[2]
            #         string += f"""(
            #              '{uid}',
            #              '{contact_uid}'
            #             ),"""
            #     # insert data into table postgres
            
            #     string = string[:len(string) - 1]
            #     try:
            #         pg_cursor.execute(
            #             f"""BEGIN;
            #                 SET CONSTRAINTS ALL DEFERRED;
            #                 INSERT INTO mcams_favoritelist (
            #                 uid_id, contact_uid_id
            #                 ) VALUES {string};
            #                 DELETE FROM mcams_favoritelist
            #                 WHERE mcams_favoritelist.uid_id IS NOT NULL AND NOT EXISTS (
            #                 SELECT 1
            #                 FROM mcams_user
            #                 WHERE mcams_user.uid = mcams_favoritelist.uid_id
            #                 ) OR NOT EXISTS (
            #                 SELECT 1
            #                 FROM mcams_user
            #                 WHERE mcams_user.uid = mcams_favoritelist.contact_uid_id 
            #                 );
            #                 COMMIT;""")
            #     except Exception as ex:
            #         print(ex)
            #     rows = my_cursor.fetchmany(10000)
            #     string = ''
            # print(time.time() - st)



            # # file
            # my_cursor.execute("SELECT * FROM `user_uploads`")
            # rows = my_cursor.fetchmany(10000)
            # string = ''

            # #while rows:
            # while rows:
            #     profile_pictures = []
            #     for row in rows:
            #         file_id = row[0]
            #         owner_id = row[1]
            #         name = row[11]
            #         row_for_old_db = f'uploads/{owner_id}/'
            #         small_file = ''
            #         middle_file = ''
            #         large_file = ''
            #         if row[4]:
            #             small_file = row_for_old_db + row[4]
            #         if row[3]:
            #             middle_file = row_for_old_db + row[3]
            #         if row[2]:
            #             large_file = row_for_old_db + row[2]
            #         datetime = row[12]
            #         size = row[10]
            #         size = size / 1024
            #         type = row[8]
            #         if type == 'image':
            #             type = 1
            #         else:
            #             type = 2
            #         check_file = row[5] + 1
            #         check_profile_image = row[7]
            #         if check_profile_image:
            #             check_profile_image = 2
            #         else:
            #             check_profile_image = 1   
            #         hash = row[9]
            #         file_link = ''
            #         if name == 'social':
            #             file_link = row[2]
            #             small_file = ''
            #             middle_file = ''
            #             large_file = ''
                    
            #         if row[6]:
            #             profile_pictures.append((owner_id, large_file))
                    
            #         string += f"""(
            #                 '{file_id}',
            #                 '{check_file}',
            #                 '{owner_id}',
            #                 '{small_file}',
            #                 '{middle_file}',
            #                 '{large_file}',
            #                 '{datetime}',
            #                 '{size}',
            #                 '{type}',
            #                 '{check_profile_image}',
            #                 '{hash}',
            #                 '{file_link}'
            #             ),"""
            #     string = string[:len(string) - 1]
            #     try:
            #         pg_cursor.execute(
            #             f"""BEGIN;
            #                 SET CONSTRAINTS ALL DEFERRED;
            #                 INSERT INTO mcams_file (
            #                 id,
            #                 check_file,
            #                 owner_id,
            #                 small_file,
            #                 middle_file,
            #                 large_file,
            #                 datetime,
            #                 size,
            #                 type,
            #                 check_profile_image,
            #                 hash,
            #                 file_link
            #                 ) VALUES {string};
            #                 DELETE FROM mcams_file
            #                 WHERE NOT EXISTS (
            #                 SELECT 1
            #                 FROM mcams_user
            #                 WHERE mcams_user.uid = mcams_file.owner_id
            #                 );
            #                 COMMIT;""")
            #     except Exception as ex:
            #          print(ex)
            #     try:
            #         for picture in profile_pictures:
            #             print(picture)
            #             pg_cursor.execute(
            #             f"""UPDATE mcams_user SET profile_picture_id = (SELECT id from mcams_file WHERE owner_id = '{picture[0]}' and large_file = '{picture[1]}' LIMIT 1) WHERE uid = '{picture[0]}'""")
            #     except Exception as ex:
            #         print(ex)
            #     rows = my_cursor.fetchmany(10000)
            #     string = ''
            # print('***' * 50)




            # # messages
            # my_cursor.execute("SELECT * FROM `messages`")
            # rows = my_cursor.fetchmany(3000)
            # st = time.time()
            # count_messages = 0
            # string = ''
            # #while rows:
            # while rows:
            #     for row in rows:
            #         count_messages += 1
            #         print(count_messages)
            #         # insert data into table postgres
            #         from_user_id = row[1]
            #         to_user_id = row[2]
            #         message = row[3].replace("'", "''")
            #         uploads_id_id = row[4]
            #         if not uploads_id_id:
            #             uploads_id_id = 'NULL'
            #         else:
            #             message = "📎"
            #         sent = row[5]
            #         read = row[6]
            #         # if not read:
            #         #    read = False
            #         date_time = row[8]
            #         link = row[10]
            #         spam = row[11]
            #         chat = row[7]
            #         support = False
                    
            #         string += f"""(
            #                 '{message}',
            #                 '{to_user_id}',
            #                 '{from_user_id}',
            #                 {uploads_id_id},
            #                 '{sent}',
            #                 '{read}',
            #                 '{date_time}',
            #                 '{link}',
            #                 '{spam}',
            #                 '{chat}',
            #                 '{support}'
            #             ),"""
            #     string = string[:len(string) - 1]
            #     try:
            #         pg_cursor.execute(
            #             f"""BEGIN;
            #                 SET CONSTRAINTS ALL DEFERRED;
            #                 INSERT INTO mcams_messages (
            #                 message,
            #                 to_user_id,
            #                 from_user_id,
            #                 uploads_id_id,
            #                 sent,
            #                 read,
            #                 date_time,
            #                 link,
            #                 spam,
            #                 chat,
            #                 support
            #                 ) VALUES {string};
            #                 UPDATE mcams_messages
            #                 SET uploads_id_id = NULL
            #                 WHERE NOT EXISTS (
            #                 SELECT 1
            #                 FROM mcams_file
            #                 WHERE mcams_file.id = mcams_messages.uploads_id_id
            #                 );
            #                 COMMIT;""")
            #     except Exception as ex:
            #          print(ex)
                    
            #     print(time.time()- st)
            #     rows = my_cursor.fetchmany(3000)
            #     string = ''
            # print('total count:', count_messages)
            # print("finish:", time.time() - st)




            # # contacts
            # my_cursor.execute("SELECT * FROM `contacts`")
            # rows = my_cursor.fetchmany(50000)
            # string = ''
            # #while rows:
            # while rows:
            #     for row in rows:
            #         uid_id = row[1]
            #         contact_uid_id = row[2]
            #         msg_id_id = row[3]
            #         last_msg = row[4].replace("'", "''")
            #         not_seen = row[6]
            #         ticket = False
                    
            #         string += f"""(
            #                 '{uid_id}',
            #                 '{contact_uid_id}',
            #                 '{msg_id_id}',
            #                 '{last_msg}',
            #                 '{not_seen}',
            #                 '{ticket}'
            #             ),"""
            #     string = string[:len(string) - 1]
            #     try:
            #         pg_cursor.execute(
            #             f"""BEGIN;
            #                 SET CONSTRAINTS ALL DEFERRED;
            #                 INSERT INTO mcams_contacts (
            #                 uid_id,
            #                 contact_uid_id,
            #                 msg_id_id,
            #                 last_msg,
            #                 not_seen,
            #                 ticket
            #                 ) VALUES {string};

            #                 DELETE FROM mcams_contacts
            #                     WHERE NOT EXISTS (
            #                     SELECT 1
            #                     FROM mcams_user
            #                     WHERE mcams_user.uid = mcams_contacts.uid_id 
            #                     ) OR NOT EXISTS (
            #                     SELECT 1
            #                     FROM mcams_user
            #                     WHERE mcams_user.uid = mcams_contacts.contact_uid_id 
            #                     );

            #                 UPDATE mcams_contacts
            #                 SET msg_id_id = ( SELECT id
            #                     FROM mcams_messages WHERE 
            #                     (mcams_messages.from_user_id = mcams_contacts.uid_id 
            #                     AND mcams_messages.to_user_id = mcams_contacts.contact_uid_id) 
            #                     OR (mcams_messages.to_user_id = mcams_contacts.uid_id 
            #                     AND mcams_messages.from_user_id = mcams_contacts.contact_uid_id) 
            #                     ORDER BY date_time DESC LIMIT 1)
            #                 WHERE mcams_contacts.msg_id_id IS NOT NULL AND NOT EXISTS (
            #                 SELECT 1
            #                 FROM mcams_messages
            #                 WHERE mcams_messages.id = mcams_contacts.msg_id_id
            #                 );
            #                 COMMIT;""")
            #     except Exception as ex:
            #          print(ex)
            #     rows = my_cursor.fetchmany(50000)
            #     print(time.time())
            #     string = ''




            # # Complaint
            # my_cursor.execute("SELECT * FROM `claims`")
            # rows = my_cursor.fetchmany(3000)
            # string = ''
            # #while rows:
            # while rows:
            #     for row in rows:
            #         from_uid_id = row[1]
            #         to_uid_id = row[2]
            #         complain = row[3]
            #         if complain == 0:
            #             complain = 'obscene'
            #         elif complain == 1:
            #             complain = 'spam'
            #         elif complain == 2:
            #             complain = 'gender'
            #         elif complain == 3:
            #             complain = 'csam'
            #         elif complain == 4:
            #             complain = 'name'
            #         elif complain == 5:
            #             complain = 'underage'
            #         date_time = row[4]
            #         read = False
                    
            #         string += f"""(
            #                 '{from_uid_id}',
            #                 '{to_uid_id}',
            #                 '{complain}',
            #                 '{date_time}',
            #                 '{read}'
            #             ),"""
            #     string = string[:len(string) - 1]
            #     try:
            #         pg_cursor.execute(
            #             f"""INSERT INTO mcams_complaint (
            #                 from_uid_id,
            #                 to_uid_id,
            #                 complain,
            #                 date_time,
            #                 read
            #                 ) VALUES {string}""")
            #     except Exception as ex:
            #          print(ex)
            #     rows = my_cursor.fetchmany(3000)
            #     string = ''



            # # banned_hash
            # my_cursor.execute("SELECT * FROM `banned_hash`")
            # rows = my_cursor.fetchmany(3000)
            # string = ''
            # #while rows:
            # while rows:
            #     for row in rows:
            #         bad_hash = row[1]
            #         uid = row[2]
            #         date_time = row[3]
            #         on_moderation = False
                    
            #         string += f"""(
            #                 '{bad_hash}',
            #                 '{uid}',
            #                 '{date_time}',
            #                 '{on_moderation}'
            #             ),"""
            #     string = string[:len(string) - 1]
            #     try:
            #         pg_cursor.execute(
            #             f"""INSERT INTO mcams_badhash (
            #                 bad_hash,
            #                 uid,
            #                 date_time,
            #                 on_moderation
            #                 ) VALUES {string}""")
            #     except Exception as ex:
            #          print(ex)
            #     rows = my_cursor.fetchmany(3000)
            #     string = ''









            # # bene_roles
            # my_cursor.execute("SELECT * FROM `bene_roles`")
            # row = my_cursor.fetchone()
            # while row is not None:
            #     # insert data into table postgres
            #     uid = row[1]
            #     bene_roles = row[2]
            #     if bene_roles == 3:
            #         bene_roles = 4
            #     elif bene_roles == 4:
            #         bene_roles = 3
            #     try:
            #         pg_cursor.execute(
            #             f"""UPDATE mcams_user SET bene_roles = {bene_roles} WHERE uid = '{uid}'""")
            #     except Exception as ex:
            #         print(ex)
            #     row = my_cursor.fetchone()



            # # users_deleted
            # my_cursor.execute("SELECT * FROM `users_deleted`")
            # row = my_cursor.fetchone()
            # while row is not None:
            #     # insert data into table postgres
            #     uid = row[1]
            #     reason_for_deletion = row[4]
            #     deletion_status = row[7]
            #     if deletion_status:
            #         deletion_status = True
            #     else:
            #         deletion_status = False
            #     try:
            #         pg_cursor.execute(COMMIT;
            #             f"""UPDATE mcams_user SET reason_for_deletion = '{reason_for_deletion}', deletion_status = {deletion_status} WHERE uid = '{uid}'""")
            #     except Exception as ex:
            #         print(ex)
            #     row = my_cursor.fetchone()


            # # bad_devices
            # my_cursor.execute("SELECT * FROM `bad_devices`")
            # rows = my_cursor.fetchmany(3000)
            # string = ''
            # #while rows:
            # while rows:
            #     for row in rows:
            #         bad_device = row[1].replace("'", "''")
                    
            #         string += f"""(
            #                 '{bad_device}'
            #             ),"""
            #     string = string[:len(string) - 1]
            #     try:
            #         pg_cursor.execute(
            #             f"""INSERT INTO mcams_baddevice (
            #                 bad_device
            #                 ) VALUES {string}""")
            #     except Exception as ex:
            #          print(ex)
            #     rows = my_cursor.fetchmany(3000)
            #     string = ''


        with mysql_conn.cursor() as my_cursor, pg_conn.cursor() as pg_cursor:
            # contact_support
            my_cursor.execute("SELECT * FROM `contact_support`")
            rows = my_cursor.fetchmany(20000)
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
                print('fetch new', time.time() - st)
                rows = my_cursor.fetchmany(20000)
                print(time.time() - st)
                string = ''


            print('ENDED constat support')
        with mysql_conn.cursor() as my_cursor, pg_conn.cursor() as pg_cursor:
            # contact_tickets
            my_cursor.execute("SELECT * FROM `contact_tickets`")
            st = time.time()
            rows = my_cursor.fetchmany(20000)
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
                print('fetch new', time.time() - st)
                rows = my_cursor.fetchmany(20000)
                print(time.time() - st)
                string = ''



    finally:
        print('EXIT')
except Exception as ex:
    print('Connection refused...')
    print(ex)
finally:
    if mysql_conn:
        mysql_conn.close()
    if pg_conn:
        pg_conn.close()


#     finally:
#         if mysql_conn:
#             mysql_conn.close()
#         if pg_conn:
#             pg_conn.close()
# except Exception as ex:
#     print('Connection refused...')
#     print(ex)

