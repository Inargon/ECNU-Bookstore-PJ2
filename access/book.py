import os
import sqlite3 as sqlite
import random
import base64
import simplejson as json


class Book:
    id: str
    title: str
    author: str
    publisher: str
    original_title: str
    translator: str
    pub_year: str
    pages: int
    price: int
    currency_unit: str
    binding: str
    isbn: str
    author_intro: str
    book_intro: str
    content: str
    tags: [str]
    pictures: [bytes]

    def __init__(self):
        self.tags = []
        self.pictures = []


class BookDB:
    def __init__(self, large: bool = False):
        parent_path = os.path.dirname(os.path.dirname(__file__))
        self.db_s = os.path.join(parent_path, "data/book.db")
        self.db_l = os.path.join(parent_path, "data/book_lx.db")
        if large:
            self.book_db = self.db_l
        else:
            self.book_db = self.db_s
        self._ensure_db_initialized()

    def _ensure_db_initialized(self):
        conn = sqlite.connect(self.book_db)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS book "
            "(id TEXT primary key, title TEXT, author TEXT, publisher TEXT, "
            "original_title TEXT, translator TEXT, pub_year TEXT, pages INTEGER, "
            "price INTEGER, currency_unit TEXT, binding TEXT, isbn TEXT, "
            "author_intro TEXT, book_intro TEXT, content TEXT, tags TEXT, picture BLOB)"
        )
        cursor = conn.execute("SELECT count(id) FROM book")
        row = cursor.fetchone()
        count = row[0] if row else 0
        if count == 0:
            # 预置少量图书数据，保证测试和基准运行
            sample = []
            for i in range(1, 501):
                book_id = f"{1000000 + i}"
                title = f"Sample Book {i}"
                author = "Author {}".format(i)
                publisher = "Publisher {}".format(i)
                original_title = title
                translator = ""
                pub_year = "2024"
                pages = 100 + i
                price = 1000 + i  # 分
                currency_unit = "CNY"
                binding = "精装"
                isbn = f"ISBN-{i}"
                author_intro = f"Author intro {i}"
                book_intro = f"Book intro {i}"
                content = f"Content {i}"
                tags = "tag{}\ntag{}".format(i % 5, (i + 1) % 5)
                sample.append(
                    (
                        book_id,
                        title,
                        author,
                        publisher,
                        original_title,
                        translator,
                        pub_year,
                        pages,
                        price,
                        currency_unit,
                        binding,
                        isbn,
                        author_intro,
                        book_intro,
                        content,
                        tags,
                        None,
                    )
                )
            conn.executemany(
                "INSERT OR IGNORE INTO book VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                sample,
            )
        conn.commit()
        conn.close()

    def get_book_count(self):
        conn = sqlite.connect(self.book_db)
        cursor = conn.execute("SELECT count(id) FROM book")
        row = cursor.fetchone()
        return row[0]

    def get_book_info(self, start, size) -> [Book]:
        books = []
        conn = sqlite.connect(self.book_db)
        cursor = conn.execute(
            "SELECT id, title, author, "
            "publisher, original_title, "
            "translator, pub_year, pages, "
            "price, currency_unit, binding, "
            "isbn, author_intro, book_intro, "
            "content, tags, picture FROM book ORDER BY id "
            "LIMIT ? OFFSET ?",
            (size, start),
        )
        for row in cursor:
            book = Book()
            book.id = row[0]
            book.title = row[1]
            book.author = row[2]
            book.publisher = row[3]
            book.original_title = row[4]
            book.translator = row[5]
            book.pub_year = row[6]
            book.pages = row[7]
            book.price = row[8]

            book.currency_unit = row[9]
            book.binding = row[10]
            book.isbn = row[11]
            book.author_intro = row[12]
            book.book_intro = row[13]
            book.content = row[14]
            tags = row[15]

            picture = row[16]

            for tag in tags.split("\n"):
                if tag.strip() != "":
                    book.tags.append(tag)
            for i in range(0, random.randint(0, 9)):
                if picture is not None:
                    encode_str = base64.b64encode(picture).decode("utf-8")
                    book.pictures.append(encode_str)
            books.append(book)
            # print(tags.decode('utf-8'))

            # print(book.tags, len(book.picture))
            # print(book)
            # print(tags)

        return books
