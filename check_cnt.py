from typing import cast
from util.db_util import get_engine
from ppl_datalake import DatalakeClient
from ppl_datalake.impl_iceberg import TableIceberg
from datetime import datetime,timedelta
import pytz
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler


logger = logging.getLogger('spark')
logger.setLevel('INFO')
handler = RotatingFileHandler('count.log', maxBytes=20*1024*1024, backupCount=1)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

db = get_engine('etc/prod_mysql.conf')
local_timezone = pytz.timezone('Asia/Shanghai')
client = DatalakeClient('c5e8791b-3ab1-4e2b-98ea-458460c95d52')
table = cast(TableIceberg, client.get_namespace('ars', group='pilot').get_table('results'))

mysql = """
    SELECT COUNT(*)
    FROM result t1 JOIN workflow t2 ON t1.workflow_id_id=t2.workflow_id
    WHERE t2.create_time>='%s' and t2.create_time<'%s'
"""

trino = """
    select COUNT(*) count from ars.results 
    where create_time>=TIMESTAMP '%s Asia/Shanghai'
    and create_time<TIMESTAMP '%s Asia/Shanghai'
"""

mysql_wf = """
    SELECT COUNT(*)
    FROM workflow
    WHERE create_time>='%s' and create_time<'%s'
"""

trino_wf = """
    select COUNT(*) count from ars.workflows
    where create_time>=TIMESTAMP '%s Asia/Shanghai'
    and create_time<TIMESTAMP '%s Asia/Shanghai'
"""

def check_cnt(start,end):
    # 看区间内count是否匹配
    with db.connect() as conn:
        mysql_cnt = conn.execute(mysql % (start,end)).fetchone()[0]
        print(f'Processing {start} - {end}')
    return mysql_cnt

def check_workflow_cnt(start,end):
    with db.connect() as conn:
        mysql_cnt = conn.execute(mysql_wf % (start,end)).fetchone()[0]
        print(f'Processing {start} - {end}')
    return mysql_cnt
    
mysql_cnt=0

check_workflow = False
last = datetime(2023,12,7)
for date in pd.date_range(last,datetime(2023,12,8)):
    # if date.day==1:
        # trino_cnt = next(table.query_by_sql(trino_wf % (last, date)))['count']
    if check_workflow:
        trino_cnt = next(table.query_by_sql(trino_wf % (last, date)))['count']
    else:
        trino_cnt = next(table.query_by_sql(trino % (last, date)))['count']
        
    if trino_cnt!=mysql_cnt:
        logger.error(f'{last} - {date}: MySQL {mysql_cnt}, Iceberg {trino_cnt} Not Match!!')
        print(f'{last} - {date}: MySQL {mysql_cnt}, Iceberg {trino_cnt} Not Match!!')
    else:
        logger.info(f'{last} - {date}: MySQL {mysql_cnt}, Iceberg {trino_cnt}')
        print(f'{last} - {date}: MySQL {mysql_cnt}, Iceberg {trino_cnt}')
    mysql_cnt = 0
    last = date
        
    for h in range(24):
        start = date + timedelta(hours=h)
        end = date + timedelta(hours=h+1)
        if check_workflow:
            mysql_cnt += check_workflow_cnt(start,end)
        else:
            mysql_cnt += check_cnt(start,end)
        
        
        
        