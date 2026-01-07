import pymysql
import uuid
import json
import logging
import time
from be.model import db_conn
from be.model import error


class Buyer(db_conn.DBConn):
    auto_cancel_seconds = 300  # 未支付超时自动取消（秒），测试可在实例上修改

    def __init__(self):
        db_conn.DBConn.__init__(self)

    def new_order(
            self, user_id: str, store_id: str, id_and_count: [(str, int)]
    ) -> (int, str, str):
        order_id = ""
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + (order_id,)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id) + (order_id,)
            uid = "{}_{}_{}".format(user_id, store_id, str(uuid.uuid1()))
            created_at = time.time()

            # 获取游标
            cursor = self.conn.cursor()

            for book_id, count in id_and_count:
                cursor.execute(
                    "SELECT book_id, stock_level, book_info FROM store "
                    "WHERE store_id = %s AND book_id = %s;",
                    (store_id, book_id),
                )
                row = cursor.fetchone()
                if row is None:
                    return error.error_non_exist_book_id(book_id) + (order_id,)

                stock_level = row[1]
                book_info = row[2]
                book_info_json = json.loads(book_info)
                price = book_info_json.get("price")

                if stock_level < count:
                    return error.error_stock_level_low(book_id) + (order_id,)

                cursor.execute(
                    "UPDATE store set stock_level = stock_level - %s "
                    "WHERE store_id = %s and book_id = %s and stock_level >= %s; ",
                    (count, store_id, book_id, count),
                )
                if cursor.rowcount == 0:
                    return error.error_stock_level_low(book_id) + (order_id,)

                cursor.execute(
                    "INSERT INTO new_order_detail(order_id, book_id, count, price) "
                    "VALUES(%s, %s, %s, %s);",
                    (uid, book_id, count, price),
                )
                cursor.execute(
                    "INSERT INTO orders_detail(order_id, book_id, count, price) "
                    "VALUES(%s, %s, %s, %s);",
                    (uid, book_id, count, price),
                )

            cursor.execute(
                "INSERT INTO new_order(order_id, store_id, user_id, created_at) "
                "VALUES(%s, %s, %s, %s);",
                (uid, store_id, user_id, created_at),
            )
            cursor.execute(
                "INSERT INTO orders(order_id, store_id, user_id, status, created_at) "
                "VALUES(%s, %s, %s, %s, %s);",
                (uid, store_id, user_id, "pending", created_at),
            )
            self.conn.commit()
            order_id = uid
        except pymysql.Error as e:
            logging.info("528, {}".format(str(e)))
            return 528, "{}".format(str(e)), ""
        except BaseException as e:
            logging.info("530, {}".format(str(e)))
            return 530, "{}".format(str(e)), ""

        return 200, "ok", order_id

    def _get_order_info(self, order_id: str):
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT order_id, user_id, store_id, status, created_at, paid_at, shipped_at, received_at "
            "FROM orders WHERE order_id = %s;",
            (order_id,),
        )
        return cursor.fetchone()

    def _auto_cancel_if_needed(self, order_row):
        if order_row is None:
            return False
        status = order_row[3]
        created_at = order_row[4]
        if status == "pending" and created_at is not None:
            if time.time() - created_at > self.auto_cancel_seconds:
                self.cancel_order(order_row[1], order_row[0], auto=True)
                return True
        return False

    def cancel_order(self, user_id: str, order_id: str, auto: bool = False):
        try:
            order_row = self._get_order_info(order_id)
            if order_row is None:
                return error.error_invalid_order_id(order_id)
            buyer_id = order_row[1]
            store_id = order_row[2]
            status = order_row[3]
            if buyer_id != user_id:
                return error.error_authorization_fail()
            if status != "pending":
                return error.error_invalid_order_id(order_id)

            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT book_id, count FROM orders_detail WHERE order_id = %s;",
                (order_id,),
            )
            # 关键修改：一次性取出所有数据，防止后续 UPDATE 时 cursor 冲突
            details = cursor.fetchall()

            # 使用新的 cursor 进行更新操作
            update_cursor = self.conn.cursor()
            for book_id, count in details:
                update_cursor.execute(
                    "UPDATE store SET stock_level = stock_level + %s "
                    "WHERE store_id = %s AND book_id = %s;",
                    (count, store_id, book_id),
                )

            update_cursor.execute(
                "UPDATE orders SET status = %s, cancel_reason = %s WHERE order_id = %s;",
                ("cancelled", "auto" if auto else "user_cancel", order_id),
            )
            update_cursor.execute("DELETE FROM new_order WHERE order_id = %s;", (order_id,))
            update_cursor.execute(
                "DELETE FROM new_order_detail WHERE order_id = %s;", (order_id,)
            )
            self.conn.commit()
        except pymysql.Error as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def payment(self, user_id: str, password: str, order_id: str) -> (int, str):
        conn = self.conn
        try:
            order_row = self._get_order_info(order_id)
            if order_row is None:
                return error.error_invalid_order_id(order_id)

            buyer_id = order_row[1]
            store_id = order_row[2]
            if self._auto_cancel_if_needed(order_row):
                return error.error_invalid_order_id(order_id)

            if order_row[3] != "pending":
                return error.error_invalid_order_id(order_id)

            if buyer_id != user_id:
                return error.error_authorization_fail()

            cursor = conn.cursor()
            cursor.execute(
                "SELECT balance, password FROM user WHERE user_id = %s;", (buyer_id,)
            )
            row = cursor.fetchone()
            if row is None:
                return error.error_non_exist_user_id(buyer_id)
            balance = row[0]
            if password != row[1]:
                return error.error_authorization_fail()

            cursor.execute(
                "SELECT store_id, user_id FROM user_store WHERE store_id = %s;",
                (store_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return error.error_non_exist_store_id(store_id)

            seller_id = row[1]

            if not self.user_id_exist(seller_id):
                return error.error_non_exist_user_id(seller_id)

            cursor.execute(
                "SELECT book_id, count, price FROM new_order_detail WHERE order_id = %s;",
                (order_id,),
            )
            total_price = 0
            # 这里必须用 fetchall 算出总价
            for row in cursor.fetchall():
                count = row[1]
                price = row[2]
                total_price = total_price + price * count

            if balance < total_price:
                return error.error_not_sufficient_funds(order_id)

            cursor.execute(
                "UPDATE user set balance = balance - %s "
                "WHERE user_id = %s AND balance >= %s",
                (total_price, buyer_id, total_price),
            )
            if cursor.rowcount == 0:
                return error.error_not_sufficient_funds(order_id)

            cursor.execute(
                "UPDATE user set balance = balance + %s " "WHERE user_id = %s",
                (total_price, seller_id),
            )

            if cursor.rowcount == 0:
                return error.error_non_exist_user_id(seller_id)

            now = time.time()
            cursor.execute(
                "UPDATE orders SET status = %s, paid_at = %s WHERE order_id = %s",
                ("paid", now, order_id),
            )
            cursor.execute("DELETE FROM new_order WHERE order_id = %s", (order_id,))
            cursor.execute("DELETE FROM new_order_detail WHERE order_id = %s", (order_id,))
            conn.commit()

        except pymysql.Error as e:
            return 528, "{}".format(str(e))

        except BaseException as e:
            return 530, "{}".format(str(e))

        return 200, "ok"

    def receive_order(self, user_id: str, order_id: str):
        try:
            order_row = self._get_order_info(order_id)
            if order_row is None:
                return error.error_invalid_order_id(order_id)
            buyer_id = order_row[1]
            status = order_row[3]
            if buyer_id != user_id:
                return error.error_authorization_fail()
            if status != "shipped":
                return error.error_invalid_order_id(order_id)
            now = time.time()
            self.conn.cursor().execute(
                "UPDATE orders SET status = %s, received_at = %s WHERE order_id = %s",
                ("received", now, order_id),
            )
            self.conn.commit()
        except pymysql.Error as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def add_funds(self, user_id, password, add_value) -> (int, str):
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT password  from user where user_id=%s", (user_id,)
            )
            row = cursor.fetchone()
            if row is None:
                return error.error_authorization_fail()

            if row[0] != password:
                return error.error_authorization_fail()

            cursor.execute(
                "UPDATE user SET balance = balance + %s WHERE user_id = %s",
                (add_value, user_id),
            )
            if cursor.rowcount == 0:
                return error.error_non_exist_user_id(user_id)

            self.conn.commit()
        except pymysql.Error as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))

        return 200, "ok"

    def list_orders(
            self, user_id: str, status: str = None, page: int = 1, page_size: int = 10
    ):
        try:
            cursor = self.conn.cursor()
            params = [user_id]
            query = "SELECT order_id, store_id, status, created_at, paid_at, shipped_at, received_at, cancel_reason FROM orders WHERE user_id = %s"
            if status:
                query += " AND status = %s"
                params.append(status)
            query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
            params.extend([page_size, (page - 1) * page_size])

            cursor.execute(query, tuple(params))
            orders = []
            for row in cursor:
                orders.append(
                    {
                        "order_id": row[0],
                        "store_id": row[1],
                        "status": row[2],
                        "created_at": row[3],
                        "paid_at": row[4],
                        "shipped_at": row[5],
                        "received_at": row[6],
                        "cancel_reason": row[7],
                    }
                )
        except pymysql.Error as e:
            return 528, "{}".format(str(e)), []
        except BaseException as e:
            return 530, "{}".format(str(e)), []
        return 200, "ok", orders

    def search_book(
            self,
            keyword: str,
            scope: str = "global",
            store_id: str = None,
            page: int = 1,
            page_size: int = 10,
    ):
        keyword_lower = f"%{keyword.lower()}%"
        results = []
        try:
            cursor = self.conn.cursor()
            params = []
            like_fields = [
                "lower(title)",
                "lower(author)",
                "lower(publisher)",
                "lower(original_title)",
                "lower(translator)",
                "lower(book_intro)",
                "lower(content)",
                "lower(catalog)",
                "lower(tags_text)",
            ]
            match_expr = " OR ".join([f"{f} LIKE %s" for f in like_fields])
            params.extend([keyword_lower] * len(like_fields))

            query = (
                "SELECT store_id, book_id, title, author, publisher, original_title, "
                "translator, book_intro, content, catalog, tags_text "
                "FROM book_search WHERE "
            )
            query += f"({match_expr})"
            if scope == "store" and store_id:
                query += " AND store_id = %s"
                params.append(store_id)
            query += " ORDER BY store_id, book_id LIMIT %s OFFSET %s"
            params.extend([page_size, (page - 1) * page_size])

            cursor.execute(query, tuple(params))
            for row in cursor:
                tags_text = row[10] if len(row) > 10 else ""
                tags_list = []
                if tags_text:
                    tags_list = tags_text.split()
                results.append(
                    {
                        "store_id": row[0],
                        "id": row[1],
                        "title": row[2],
                        "author": row[3],
                        "publisher": row[4],
                        "original_title": row[5],
                        "translator": row[6],
                        "book_intro": row[7],
                        "content": row[8],
                        "catalog": row[9],
                        "tags": tags_list,
                    }
                )
            return 200, "ok", results
        except pymysql.Error as e:
            return 528, "{}".format(str(e)), []
        except BaseException as e:
            return 530, "{}".format(str(e)), []