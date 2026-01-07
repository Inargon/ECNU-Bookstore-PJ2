from fe import conf
from fe.access import seller, auth
import time


def register_new_seller(user_id, password) -> seller.Seller:
    a = auth.Auth(conf.URL)
    code = None
    for _ in range(10):
        code = a.register(user_id, password)
        if code == 200:
            break
        time.sleep(0.1)
    assert code == 200
    s = seller.Seller(conf.URL, user_id, password)
    return s
