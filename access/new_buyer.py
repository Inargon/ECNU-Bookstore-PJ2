from fe import conf
from fe.access import buyer, auth
import time


def register_new_buyer(user_id, password) -> buyer.Buyer:
    a = auth.Auth(conf.URL)
    code = None
    for _ in range(10):
        code = a.register(user_id, password)
        if code == 200:
            break
        time.sleep(0.1)
    assert code == 200
    s = buyer.Buyer(conf.URL, user_id, password)
    return s
