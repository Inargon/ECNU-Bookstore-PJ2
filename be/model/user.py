import time
import logging
import pymysql # 改用 pymysql
from be.model import error
from be.model import db_conn

# 由于测试只需要校验 token 是否匹配并在有效期内，无需引入外部 JWT 依赖，
# 直接用“随机串:时间戳”的格式生成、校验 token。
def jwt_encode(user_id: str, terminal: str) -> str:
    ts = time.time()
    return f"{user_id}:{terminal}:{ts}"


def jwt_decode(encoded_token: str, user_id: str) -> dict:
    try:
        user, terminal, ts = encoded_token.split(":", 2)
        return {"user_id": user, "terminal": terminal, "timestamp": float(ts)}
    except Exception:
        raise ValueError("invalid token format")


class User(db_conn.DBConn):
    token_lifetime: int = 3600  # 3600 second

    def __init__(self):
        db_conn.DBConn.__init__(self)

    def __check_token(self, user_id, db_token, token) -> bool:
        try:
            if db_token != token:
                return False
            jwt_text = jwt_decode(encoded_token=token, user_id=user_id)
            ts = jwt_text["timestamp"]
            if ts is not None:
                now = time.time()
                if self.token_lifetime > now - ts >= 0:
                    return True
        except Exception as e:
            logging.error(str(e))
            return False

    def register(self, user_id: str, password: str):
        for _ in range(5):
            try:
                terminal = "terminal_{}".format(str(time.time()))
                token = jwt_encode(user_id, terminal)
                # 修改占位符为 %s
                self.conn.cursor().execute(
                    "INSERT into user(user_id, password, balance, token, terminal) "
                    "VALUES (%s, %s, %s, %s, %s);",
                    (user_id, password, 0, token, terminal),
                )
                self.conn.commit()
                return 200, "ok"
            except pymysql.Error as e: # 修改异常类型
                msg = str(e).lower()
                # MySQL 的死锁或锁定错误码通常是 1205 或 1213
                if "lock" in msg or "deadlock" in msg:
                    time.sleep(0.1)
                    continue
                if "duplicate" in msg:
                    return error.error_exist_user_id(user_id)
                return 528, "{}".format(str(e))
        return 528, "database locked"

    def check_token(self, user_id: str, token: str) -> (int, str):
        cursor = self.conn.cursor()
        cursor.execute("SELECT token from user where user_id=%s", (user_id,))
        row = cursor.fetchone()
        if row is None:
            return error.error_authorization_fail()
        db_token = row[0]
        if not self.__check_token(user_id, db_token, token):
            return error.error_authorization_fail()
        return 200, "ok"

    def check_password(self, user_id: str, password: str) -> (int, str):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT password from user where user_id=%s", (user_id,)
        )
        row = cursor.fetchone()
        if row is None:
            return error.error_authorization_fail()

        if password != row[0]:
            return error.error_authorization_fail()

        return 200, "ok"

    def login(self, user_id: str, password: str, terminal: str) -> (int, str, str):
        token = ""
        try:
            code, message = self.check_password(user_id, password)
            if code != 200:
                return code, message, ""

            token = jwt_encode(user_id, terminal)
            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE user set token= %s , terminal = %s where user_id = %s",
                (token, terminal, user_id),
            )
            if cursor.rowcount == 0:
                return error.error_authorization_fail() + ("",)
            self.conn.commit()
        except pymysql.Error as e:
            return 528, "{}".format(str(e)), ""
        except BaseException as e:
            return 530, "{}".format(str(e)), ""
        return 200, "ok", token

    def logout(self, user_id: str, token: str) -> bool:
        try:
            code, message = self.check_token(user_id, token)
            if code != 200:
                return code, message

            terminal = "terminal_{}".format(str(time.time()))
            dummy_token = jwt_encode(user_id, terminal)

            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE user SET token = %s, terminal = %s WHERE user_id=%s",
                (dummy_token, terminal, user_id),
            )
            if cursor.rowcount == 0:
                return error.error_authorization_fail()

            self.conn.commit()
        except pymysql.Error as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def unregister(self, user_id: str, password: str) -> (int, str):
        try:
            code, message = self.check_password(user_id, password)
            if code != 200:
                return code, message

            cursor = self.conn.cursor()
            cursor.execute("DELETE from user where user_id=%s", (user_id,))
            if cursor.rowcount == 1:
                self.conn.commit()
            else:
                return error.error_authorization_fail()
        except pymysql.Error as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def change_password(
        self, user_id: str, old_password: str, new_password: str
    ) -> bool:
        try:
            code, message = self.check_password(user_id, old_password)
            if code != 200:
                return code, message

            terminal = "terminal_{}".format(str(time.time()))
            token = jwt_encode(user_id, terminal)
            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE user set password = %s, token= %s , terminal = %s where user_id = %s",
                (new_password, token, terminal, user_id),
            )
            if cursor.rowcount == 0:
                return error.error_authorization_fail()

            self.conn.commit()
        except pymysql.Error as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"