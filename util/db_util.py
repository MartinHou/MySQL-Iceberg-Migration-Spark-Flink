import configparser
import sqlalchemy


def get_engine(path):
    configer = configparser.ConfigParser()
    configer.read(path)
    src_config = dict(dict(configer["client"]))
    src_connection_str = f"mysql+pymysql://{src_config.get('user')}:{src_config.get('password')}@{src_config.get('host')}:{src_config.get('port')}/{src_config.get('database')}"
    engine = sqlalchemy.create_engine(
        src_connection_str, pool_pre_ping=True, pool_size=1, pool_recycle=30)
    return engine