from typing import cast
from util.db_util import get_engine
from ppl_datalake import DatalakeClient
from ppl_datalake.impl_iceberg import TableIceberg
from datetime import datetime
import pytz
import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger('spark')
logger.setLevel('INFO')
handler = RotatingFileHandler('check.log', maxBytes=20*1024*1024, backupCount=1)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def check_result():
    db = get_engine('etc/prod_mysql.conf')
    sql = """
        SELECT * FROM result WHERE id = "%s";
    """
    local_timezone = pytz.timezone('Asia/Shanghai')
    client = DatalakeClient('c5e8791b-3ab1-4e2b-98ea-458460c95d52')
    table = cast(TableIceberg, client.get_namespace('ars', group='pilot').get_table('results'))
    
    cnt = 1
    with db.connect() as conn:
        for doc in table.query_by_sql(f'select * from ars.results'):
            result = conn.execute(sql % doc['id']).fetchone()
            for fields in ('id', 'input_md5', 'output_md5', 'log', 'metric', 'create_time', 'update_time', 'workflow_id_id', 'error_details', 'error_stage', 'error_type'):
                if fields == 'create_time' or fields == 'update_time':
                    db_time = result[fields].astimezone(local_timezone)
                    iceberg_time = doc[fields].astimezone(local_timezone)
                    assert db_time == iceberg_time, f'{fields} not equal.\n\nDB:{db_time}\n\nIceberg:{iceberg_time}'
                else:
                    assert result[fields] == doc[fields], f'{fields} not equal.\n\nDB:{result[fields]}\n\nIceberg:{doc[fields]}'
            print(f'ok {cnt}')
            cnt+=1
            
def check_workflow():
    db = get_engine('etc/prod_mysql.conf')
    sql = """
        SELECT * FROM workflow WHERE workflow_id = "%s";
    """
    local_timezone = pytz.timezone('Asia/Shanghai')
    client = DatalakeClient('c5e8791b-3ab1-4e2b-98ea-458460c95d52')
    table = cast(TableIceberg, client.get_namespace('ars', group='pilot').get_table('workflows'))
    
    cnt = 1
    with db.connect() as conn:
        for doc in table.query_by_sql(f'select * from ars.workflows'):
            result = conn.execute(sql % doc['workflow_id']).fetchone()
            for fields in ('workflow_id', 'workflow_type', 'workflow_name', 'user', 'workflow_input', 'workflow_output', 'log', 'workflow_status', 'priority', '_tag', 'create_time', 'update_time', 'batch_id_id', 'hook', 'device', 'tos_id', 'device_num', 'data_source', 'category', 'upload_ttl', 'bag_nums', 'metric'):
                if fields == 'create_time' or fields == 'update_time':
                    db_time = result[fields].astimezone(local_timezone)
                    iceberg_time = doc[fields].astimezone(local_timezone)
                    assert db_time == iceberg_time, f'{fields} not equal.\n\nDB:{db_time}\n\nIceberg:{iceberg_time}'
                elif fields=='_tag':
                    assert result['tag'] == doc['_tag'], f'{fields} not equal.\n\nDB:{result[fields]}\n\nIceberg:{doc[fields]}'
                else:
                    assert result[fields] == doc[fields], f'{fields} not equal.\n\nDB:{result[fields]}\n\nIceberg:{doc[fields]}'
            print(f'ok {cnt}')
            cnt+=1

if __name__ == '__main__':
    # check_result()
    check_workflow()
        
        