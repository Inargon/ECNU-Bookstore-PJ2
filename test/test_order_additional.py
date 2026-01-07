import time
import uuid
import pytest

from fe.access.book import Book
from fe.access.new_buyer import register_new_buyer
from fe.access.new_seller import register_new_seller


def _make_book(book_id: str, title: str, price: int = 1000) -> Book:
    bk = Book()
    bk.id = book_id
    bk.title = title
    bk.author = "author"
    bk.publisher = "publisher"
    bk.price = price
    bk.original_title = title
    bk.translator = ""
    bk.pub_year = "2024"
    bk.pages = 100
    bk.currency_unit = "CNY"
    bk.binding = "精装"
    bk.isbn = f"isbn-{book_id}"
    bk.author_intro = "intro"
    bk.book_intro = "book intro"
    bk.content = "content keyword"
    return bk


class TestOrderShipReceive:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.seller_id = f"seller_additional_{uuid.uuid1()}"
        self.store_id = f"store_additional_{uuid.uuid1()}"
        self.buyer_id = f"buyer_additional_{uuid.uuid1()}"
        self.password = "pwd_" + self.seller_id
        self.seller = register_new_seller(self.seller_id, self.password)
        assert self.seller.create_store(self.store_id) == 200
        self.book = _make_book("900001", "ShipFlow Keyword", 500)
        assert self.seller.add_book(self.store_id, 2, self.book) == 200
        self.buyer = register_new_buyer(self.buyer_id, self.password)
        self.total_price = self.book.price * 1
        yield

    def test_ship_without_pay_then_repeated_ship(self):
        code, order_id = self.buyer.new_order(self.store_id, [(self.book.id, 1)])
        assert code == 200

        # 未支付发货应失败
        ship_code = self.seller.ship_order(self.store_id, order_id)
        assert ship_code != 200

        # 支付后首次发货成功
        assert self.buyer.add_funds(self.total_price) == 200
        assert self.buyer.payment(order_id) == 200
        ship_code = self.seller.ship_order(self.store_id, order_id)
        assert ship_code == 200

        # 重复发货失败
        ship_code = self.seller.ship_order(self.store_id, order_id)
        assert ship_code != 200

    def test_receive_and_list_status(self):
        code, order_id = self.buyer.new_order(self.store_id, [(self.book.id, 1)])
        assert code == 200
        assert self.buyer.add_funds(self.total_price) == 200
        assert self.buyer.payment(order_id) == 200
        assert self.seller.ship_order(self.store_id, order_id) == 200
        assert self.buyer.receive_order(order_id) == 200

        # 列出已收货订单
        code, orders = self.buyer.list_orders(status="received")
        assert code == 200
        assert any(o["order_id"] == order_id and o["status"] == "received" for o in orders)

        # 已支付订单列表不应包含已收货订单
        code, paid_orders = self.buyer.list_orders(status="paid")
        assert code == 200
        assert all(o["order_id"] != order_id for o in paid_orders)

    def test_cancel_after_pay_should_fail(self):
        code, order_id = self.buyer.new_order(self.store_id, [(self.book.id, 1)])
        assert code == 200
        assert self.buyer.add_funds(self.total_price) == 200
        assert self.buyer.payment(order_id) == 200
        cancel_code = self.buyer.cancel_order(order_id)
        assert cancel_code != 200


class TestSearchAndPagination:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.seller_id = f"seller_search_{uuid.uuid1()}"
        self.store_a = f"store_search_a_{uuid.uuid1()}"
        self.store_b = f"store_search_b_{uuid.uuid1()}"
        self.password = "pwd_" + self.seller_id
        self.seller = register_new_seller(self.seller_id, self.password)
        assert self.seller.create_store(self.store_a) == 200
        assert self.seller.create_store(self.store_b) == 200
        book_a1 = _make_book("910001", "Alpha Keyword One", 300)
        book_a1.tags = ["TagSearch"]
        book_a1.content = "ContentSearch hits"

        book_a2 = _make_book("910002", "Alpha Other", 300)
        book_a2.tags = ["OtherTag"]
        book_a2.catalog = "CatalogSearch"

        book_b1 = _make_book("920001", "Beta Keyword Two", 300)
        assert self.seller.add_book(self.store_a, 3, book_a1) == 200
        assert self.seller.add_book(self.store_a, 3, book_a2) == 200
        assert self.seller.add_book(self.store_b, 3, book_b1) == 200
        self.buyer = register_new_buyer(f"buyer_search_{uuid.uuid1()}", self.password)
        yield

    def test_search_scope_and_pagination(self):
        # 店铺内搜索
        code, books_store = self.buyer.search("Keyword", scope="store", store_id=self.store_a)
        assert code == 200
        assert all(b["store_id"] == self.store_a for b in books_store)
        assert any("Keyword" in b.get("title", "") for b in books_store)

        # 全局搜索，至少命中两本（A 和 B）
        code, books_global = self.buyer.search("Keyword", scope="global")
        assert code == 200
        assert len(books_global) >= 2

        # 分页：page_size=1 应只返回1条，第二页也应有结果
        code, page1 = self.buyer.search("Keyword", scope="global", page=1, page_size=1)
        assert code == 200
        assert len(page1) == 1

        code, page2 = self.buyer.search("Keyword", scope="global", page=2, page_size=1)
        assert code == 200
        combined = page1 + page2
        assert len(combined) >= 2

    def test_search_by_tags_and_catalog(self):
        # 标签匹配
        code, books = self.buyer.search("TagSearch", scope="store", store_id=self.store_a)
        assert code == 200
        assert any("TagSearch".lower() in " ".join(b.get("tags", [])).lower() for b in books)

        # 目录/内容匹配
        code, books_catalog = self.buyer.search("CatalogSearch", scope="store", store_id=self.store_a)
        assert code == 200
        assert any("catalogsearch" in str(b.get("catalog", "")).lower() for b in books_catalog)
