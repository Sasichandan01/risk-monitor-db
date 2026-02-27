import logging
import threading
import time
import psycopg2
from psycopg2.pool import SimpleConnectionPool

logger = logging.getLogger(__name__)


class BaseWriter:

    def __init__(self, connection_string, option_type):
        self.connection_string = connection_string.strip() if connection_string else None
        self.option_type = option_type
        self.pool = None
        self.running = True

        if not self.connection_string:
            raise ValueError("Missing connection string for %s" % option_type)

        self.connect()
        self.start_keep_alive()

    def connect(self):
        try:
            self.pool = SimpleConnectionPool(minconn=1, maxconn=5, dsn=self.connection_string)
            logger.info("%s DB pool connected", self.option_type)
        except psycopg2.OperationalError as e:
            logger.error("%s DB connection failed: %s", self.option_type, e)
            raise

    def start_keep_alive(self):
        def ping():
            while self.running:
                time.sleep(30)
                try:
                    conn = self.pool.getconn()
                    conn.cursor().execute("SELECT 1;")
                    self.pool.putconn(conn)
                except Exception as e:
                    logger.warning("%s ping failed — reconnecting: %s", self.option_type, e)
                    try:
                        self.pool.closeall()
                    except Exception:
                        pass
                    try:
                        self.connect()
                        logger.info("%s reconnected", self.option_type)
                    except Exception as re:
                        logger.error("%s reconnect failed: %s", self.option_type, re)

        threading.Thread(target=ping, daemon=True, name="%s-ping" % self.option_type).start()
        logger.info("%s DB keep-alive started", self.option_type)

    def close(self):
        self.running = False
        if self.pool:
            try:
                self.pool.closeall()
                logger.info("%s DB disconnected", self.option_type)
            except psycopg2.Error as e:
                logger.warning("%s disconnect error: %s", self.option_type, e)