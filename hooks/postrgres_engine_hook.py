from airflow.hooks.base_hook import BaseHook
from sqlalchemy.engine import create_engine

class postgres_hook(BaseHook):
    def __init__(self, conn_id, database):
        super().__init__(source=None)
        self.conn_id = conn_id
        self.database = database

    def get_engine(self):
        config = self.get_connection(self.conn_id)
        host = config.host
        user = config.login
        passwd = config.password

        conn_string = 'postgresql+psycopg2://{}:{}@{}/{}'.format(user, passwd, host, self.database)
        return create_engine(conn_string)
