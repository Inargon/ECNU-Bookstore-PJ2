import os
import sqlite3
import time
import uuid
import pytest
import pymysql  # <--- 引入 pymysql

from fe import conf
from fe.access import book
from fe.access.new_seller import register_new_seller
from fe.access.new_buyer import register_new_buyer


def _get_be_db_path():
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    return os.path.join(project_root, "be.db")


class TestOrderFlow:
    @pytest.fixture(autouse=True)
    def setup_env(self):
        self.seller_id = f"seller_flow_{uuid.uuid1()}"
        self.store_id = f"store_flow_{uuid.uuid1()}"
        self.buyer_id = f"buyer_flow_{uuid.uuid1()}"
        self.password = "pwd_" + self.seller_id
        self.seller = register_new_seller(self.seller_id, self.password)
        assert self.seller.create_store(self.store_id) == 200

        db = book.BookDB(conf.Use_Large_DB)
        self.book = db.get_book_info(0, 1)[0]
        self.stock_level = 5
        assert self.seller.add_book(self.store_id, self.stock_level, self.book) == 200

        self.buyer = register_new_buyer(self.buyer_id, self.password)
        self.single_price = self.book.price if self.book.price else 100
        self.total_price = self.single_price * 2
        yield

    def _new_order(self):
        code, order_id = self.buyer.new_order(self.store_id, [(self.book.id, 2)])
        assert code == 200
        return order_id

    def test_full_ship_receive(self):
        order_id = self._new_order()
        assert self.buyer.add_funds(self.total_price) == 200
        assert self.buyer.payment(order_id) == 200
        assert self.seller.ship_order(self.store_id, order_id) == 200
        assert self.buyer.receive_order(order_id) == 200
        code, orders = self.buyer.list_orders()
        assert code == 200
        target = [o for o in orders if o["order_id"] == order_id][0]
        assert target["status"] == "received"

    def test_cancel_pending(self):
        order_id = self._new_order()
        assert self.buyer.cancel_order(order_id) == 200
        # 支付应失败
        assert self.buyer.payment(order_id) != 200
        code, orders = self.buyer.list_orders(status="cancelled")
        assert code == 200
        assert any(o["order_id"] == order_id for o in orders)

    def test_auto_cancel_timeout(self):
        order_id = self._new_order()

        # === 修改部分开始: 连接 MySQL 修改订单时间 ===
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='123456',  # <--- 已修改为你提供的密码
            database='bookstore',
            autocommit=True
        )

        # 将创建时间改为很早，触发超时
        too_old = time.time() - 3600

        cursor = conn.cursor()
        # 注意：使用 %s 占位符
        cursor.execute("UPDATE new_order SET created_at = %s WHERE order_id = %s", (too_old, order_id))
        cursor.execute("UPDATE orders SET created_at = %s WHERE order_id = %s", (too_old, order_id))
        conn.commit()
        conn.close()
        # === 修改部分结束 ===

        assert self.buyer.payment(order_id) != 200
        code, orders = self.buyer.list_orders(status="cancelled")
        assert code == 200
        assert any(o["order_id"] == order_id for o in orders)


class TestSearchBooks:
    @pytest.fixture(autouse=True)
    def setup_env(self):
        self.seller_id = f"seller_search_{uuid.uuid1()}"
        self.store_id = f"store_search_{uuid.uuid1()}"
        self.password = "pwd_" + self.seller_id
        self.seller = register_new_seller(self.seller_id, self.password)
        assert self.seller.create_store(self.store_id) == 200
        db = book.BookDB(conf.Use_Large_DB)
        self.books = db.get_book_info(0, 3)
        for bk in self.books:
            assert self.seller.add_book(self.store_id, 3, bk) == 200
        self.buyer = register_new_buyer(f"buyer_search_{uuid.uuid1()}", self.password)
        yield

    def test_search_by_title(self):
        keyword = self.books[0].title.split(" ")[0]
        code, books_found = self.buyer.search(keyword, scope="store", store_id=self.store_id)
        assert code == 200
        assert any(keyword.lower() in b.get("title", "").lower() for b in books_found)