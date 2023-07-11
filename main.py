import time

from trancfer_bad_fraud_words import tr_bad_fraud_words
from transfer_bad_devices import tr_bad_devices
from transfer_bad_fraud import tr_bad_fraud
from transfer_bad_spam import tr_bad_spam
from transfer_bad_words import tr_bad_words
from transfer_badhash import tr_badhash
from transfer_bene_roles import tr_bene_roles
from transfer_black_list import tr_black_list
from transfer_complaint import tr_complaint
from transfer_contact_support import tr_contact_support
from transfer_contact_ticket import tr_contact_tickets
from transfer_contacts import tr_contacts
from transfer_favorite_list import tr_favorite_list
from transfer_file import tr_file
from transfer_messages import tr_messages
from transfer_users import tr_users
from transfer_users_deleted import tr_users_deleted

amount = 50000

if __name__ == "__main__":
    print('start')
    st = time.time()
    tr_bad_fraud(amount)
    print('bad_fraud : DONE')
    tr_bad_fraud_words(amount)
    print('bad_fraud_words : DONE')
    tr_bad_spam(amount)
    print('bad_spam : DONE')
    tr_bad_words(amount)
    print('bad_words : DONE')
    tr_users(amount)
    print('users : DONE')
    tr_badhash(amount)
    print('tr_badhash : DONE')
    tr_bene_roles(10000)
    print('tr_bene_roles : DONE')
    tr_users_deleted(amount)
    print('tr_users_deleted : DONE')
    tr_bad_devices(amount)
    print('tr_bad_devices : DONE')
    tr_black_list(10000)
    print('black_list : DONE')
    tr_favorite_list(10000)
    print('favorite_list : DONE')
    tr_complaint(amount)
    print('tr_complaint : DONE')
    tr_file(5000)
    print('tr_file : DONE')
    tr_messages(amount)
    print('tr_messages : DONE')
    tr_contacts(10000)
    print('tr_contacts : DONE')
    tr_contact_support(amount)
    print('tr_contact_support : DONE')
    tr_contact_tickets(amount)
    print('tr_contact_tickets : DONE')
    print("ALL TIME :", time.time() - st)
