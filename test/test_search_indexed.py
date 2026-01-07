import uuid
import pytest

from fe.access.book import Book
from fe.access.new_seller import register_new_seller
from fe.access.new_buyer import register_new_buyer


def _make_book(book_id: str, title: str, tags=None, catalog=None, content=None) -> Book:
    bk = Book()
    bk.id = book_id
    bk.title = title
    bk.author = "author"
    bk.publisher = "publisher"
    bk.original_title = title
    bk.translator = ""
    bk.pub_year = "2024"
    bk.pages = 100
    bk.price = 1000
    bk.currency_unit = "CNY"
    bk.binding = "精装"
    bk.isbn = f"isbn-{book_id}"
    bk.author_intro = "intro"
    bk.book_intro = "book intro"
    bk.content = content or "content keyword"
    bk.tags = tags or []
    if catalog:
        bk.catalog = catalog
    return bk


class TestSearchIndexed:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.seller_id = f"seller_indexed_{uuid.uuid1()}"
        self.store_id = f"store_indexed_{uuid.uuid1()}"
        self.password = "pwd_" + self.seller_id
        self.seller = register_new_seller(self.seller_id, self.password)
        assert self.seller.create_store(self.store_id) == 200
        bk1 = _make_book("930001", "Indexed Title A", tags=["TagA"], catalog="CatA1")
        bk2 = _make_book("930002", "Indexed Title B", tags=["TagB"], catalog="CatB1")
        bk3 = _make_book("930003", "Other Title", tags=["Mixed", "TagA"], content="DeepContentX")
        assert self.seller.add_book(self.store_id, 2, bk1) == 200
        assert self.seller.add_book(self.store_id, 2, bk2) == 200
        assert self.seller.add_book(self.store_id, 2, bk3) == 200
        self.buyer = register_new_buyer(f"buyer_indexed_{uuid.uuid1()}", self.password)
        yield

    def test_search_tags_and_content(self):
        code, res = self.buyer.search("taga", scope="store", store_id=self.store_id)
        assert code == 200
        assert any("taga" in " ".join(item.get("tags", [])).lower() for item in res)

        code, res_content = self.buyer.search("deepcontentx", scope="store", store_id=self.store_id)
        assert code == 200
        assert any("deepcontentx" in item.get("content", "").lower() for item in res_content)

    def test_search_catalog_and_pagination(self):
        code, res = self.buyer.search("cata1", scope="store", store_id=self.store_id, page_size=1)
        assert code == 200
        assert len(res) == 1
        assert any("cata1" in item.get("catalog", "").lower() for item in res)

        code, res_page2 = self.buyer.search("cata1", scope="store", store_id=self.store_id, page=2, page_size=1)
        assert code == 200
        # page2 may be empty if only one match; ensure page1 at least exists and page2 doesn't crash
        assert len(res_page2) in (0, 1)
