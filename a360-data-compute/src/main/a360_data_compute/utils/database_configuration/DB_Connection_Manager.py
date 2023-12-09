import psycopg2
from psycopg2 import pool
import co


class PostgreDB:
    CONN_POOL = None

    def __init__(self):
        self.ps_connection = self.CONN_POOL.getconn()

    def __del__(self):
        self.CONN_POOL.putconn(self.ps_connection)

    @classmethod
    def create_pool(cls):
    #     params = config()
    #     try:
    #         cls.CONN_POOL = psycopg2.pool.ThreadedConnectionPool(5, 20, **params)
    #         if cls.CONN_POOL:
    #             print("Connection pool created successfully using ThreadedConnectionPool")
    #     except (Exception, psycopg2.DatabaseError) as error:
    #         print("Error while connecting to PostgreSQL", error)
    #
    # @classmethod
    # def release_pool(cls):
    #     if cls.CONN_POOL:
    #         var = cls.CONN_POOL.closeall
    #     print("Threaded PostgreSQL connection pool is closed")

