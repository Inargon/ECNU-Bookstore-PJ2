import logging
import os
import pymysql
import threading


class Store:
    def __init__(self, db_path):
        # MySQL 不需要 db_path 文件路径，但保留参数以兼容旧代码接口
        self.init_tables()

    def init_tables(self):
        try:
            conn = self.get_db_conn()
            with conn.cursor() as cursor:
                # 1. 用户表
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS user ("
                    "user_id VARCHAR(255) PRIMARY KEY, password TEXT NOT NULL, "
                    "balance INTEGER NOT NULL, token TEXT, terminal TEXT);"
                )

                # 2. 用户-店铺关联表
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS user_store("
                    "user_id VARCHAR(255), store_id VARCHAR(255), "
                    "PRIMARY KEY(user_id, store_id));"
                )

                # 3. 店铺表 (存储书籍基本信息)
                # book_info 是 JSON 字符串，MySQL 5.7+ 支持 JSON 类型，这里用 LONGTEXT 兼容性更好
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS store( "
                    "store_id VARCHAR(255), book_id VARCHAR(255), "
                    "book_info LONGTEXT, stock_level INTEGER,"
                    " PRIMARY KEY(store_id, book_id))"
                )

                # 4. 新订单表
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS new_order( "
                    "order_id VARCHAR(255) PRIMARY KEY, user_id VARCHAR(255), "
                    "store_id VARCHAR(255), created_at DOUBLE)"
                )

                # 5. 新订单详情表
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS new_order_detail( "
                    "order_id VARCHAR(255), book_id VARCHAR(255), "
                    "count INTEGER, price INTEGER, "
                    "PRIMARY KEY(order_id, book_id))"
                )

                # 6. 历史订单表
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS orders( "
                    "order_id VARCHAR(255) PRIMARY KEY, user_id VARCHAR(255), "
                    "store_id VARCHAR(255), status VARCHAR(50), "
                    "created_at DOUBLE, paid_at DOUBLE, "
                    "shipped_at DOUBLE, received_at DOUBLE, cancel_reason TEXT)"
                )

                # 7. 历史订单详情表
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS orders_detail( "
                    "order_id VARCHAR(255), book_id VARCHAR(255), "
                    "count INTEGER, price INTEGER, "
                    "PRIMARY KEY(order_id, book_id))"
                )

                # 8. 搜索专用表 (全文检索)
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS book_search("
                    "store_id VARCHAR(255), book_id VARCHAR(255), "
                    "title TEXT, author TEXT, publisher TEXT, "
                    "original_title TEXT, translator TEXT, "
                    "book_intro TEXT, content TEXT, "
                    "catalog TEXT, tags_text TEXT, "
                    "PRIMARY KEY(store_id, book_id))"
                )

                # 创建索引 (MySQL 语法)
                # 注意：TEXT 类型做索引通常需要指定长度，或者使用全文索引
                # 这里为了简单，我们只对 Text 的前 255 个字符建索引，或者依赖 LIKE 搜索
                # 如果需要高性能全文搜索，可以使用 FULLTEXT 索引，这里暂按原逻辑保留普通索引尝试
                # MySQL 对于 TEXT 列建索引必须指定长度
                try:
                    cursor.execute("CREATE INDEX idx_book_search_title ON book_search(title(255))")
                    cursor.execute("CREATE INDEX idx_book_search_author ON book_search(author(255))")
                    cursor.execute("CREATE INDEX idx_book_search_tags ON book_search(tags_text(255))")
                except Exception as e:
                    # 索引可能已存在，忽略错误
                    logging.info(f"Index creation info: {e}")
                    pass

            conn.commit()
        except pymysql.Error as e:
            logging.error(e)
            # conn.rollback() # 刚连接可能还没事务，视情况而定

    def get_db_conn(self) -> pymysql.connections.Connection:
        # === 配置你的 MySQL 连接信息 ===
        return pymysql.connect(
            host='localhost',
            user='root',  # 你的 MySQL 用户名
            password='123456',  # 你的 MySQL 密码
            database='bookstore',  # 你的数据库名
            charset='utf8mb4',
            autocommit=False  # 保持手动 commit，符合原有逻辑
        )


database_instance: Store = None
init_completed_event = threading.Event()


def init_database(db_path):
    global database_instance
    database_instance = Store(db_path)


def get_db_conn():
    global database_instance
    return database_instance.get_db_conn()