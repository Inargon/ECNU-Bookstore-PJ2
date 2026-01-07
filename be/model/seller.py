import pymysql
import time
from be.model import error
from be.model import db_conn


class Seller(db_conn.DBConn):
    def __init__(self):
        db_conn.DBConn.__init__(self)

    def add_book(
            self,
            user_id: str,
            store_id: str,
            book_id: str,
            book_json_str: str,
            stock_level: int,
    ):
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id)
            if self.book_id_exist(store_id, book_id):
                return error.error_exist_book_id(book_id)

            self.conn.cursor().execute(
                "INSERT into store(store_id, book_id, book_info, stock_level)"
                "VALUES (%s, %s, %s, %s)",
                (store_id, book_id, book_json_str, stock_level),
            )
            # 维护搜索表，抽取可索引字段
            try:
                import json

                info = json.loads(book_json_str)
                tags = info.get("tags", [])
                if isinstance(tags, str):
                    tags_text = tags
                else:
                    tags_text = " ".join(tags)
                catalog = info.get("catalog", "")

                # SQLite 的 INSERT OR REPLACE 改为 MySQL 的 REPLACE INTO
                self.conn.cursor().execute(
                    "REPLACE INTO book_search("
                    "store_id, book_id, title, author, publisher, original_title, "
                    "translator, book_intro, content, catalog, tags_text)"
                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        store_id,
                        book_id,
                        info.get("title", ""),
                        info.get("author", ""),
                        info.get("publisher", ""),
                        info.get("original_title", ""),
                        info.get("translator", ""),
                        info.get("book_intro", ""),
                        info.get("content", ""),
                        catalog,
                        tags_text,
                    ),
                )
            except Exception as e:
                # 搜索表非核心流程，异常不影响主事务
                pass
            self.conn.commit()
        except pymysql.Error as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def ship_order(self, user_id: str, store_id: str, order_id: str):
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)

            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT user_id FROM user_store WHERE store_id = %s", (store_id,)
            )
            row = cursor.fetchone()
            if row is None:
                return error.error_non_exist_store_id(store_id)
            if row[0] != user_id:
                return error.error_authorization_fail()

            cursor.execute(
                "SELECT status FROM orders WHERE order_id = %s AND store_id = %s",
                (order_id, store_id),
            )
            row = cursor.fetchone()
            if row is None or row[0] != "paid":
                return error.error_invalid_order_id(order_id)
            now = time.time()
            cursor.execute(
                "UPDATE orders SET status = %s, shipped_at = %s WHERE order_id = %s",
                ("shipped", now, order_id),
            )
            self.conn.commit()
        except pymysql.Error as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def add_stock_level(
            self, user_id: str, store_id: str, book_id: str, add_stock_level: int
    ):
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id)
            if not self.book_id_exist(store_id, book_id):
                return error.error_non_exist_book_id(book_id)

            self.conn.cursor().execute(
                "UPDATE store SET stock_level = stock_level + %s "
                "WHERE store_id = %s AND book_id = %s",
                (add_stock_level, store_id, book_id),
            )
            self.conn.commit()
        except pymysql.Error as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def create_store(self, user_id: str, store_id: str) -> (int, str):
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)
            if self.store_id_exist(store_id):
                return error.error_exist_store_id(store_id)
            self.conn.cursor().execute(
                "INSERT into user_store(store_id, user_id)" "VALUES (%s, %s)",
                (store_id, user_id),
            )
            self.conn.commit()
        except pymysql.Error as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"