import pandas as pd
from util.db_util import get_engine
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler

db = get_engine('etc/prod_mysql.conf')

logger = logging.getLogger('spark')
logger.setLevel('INFO')
handler = RotatingFileHandler('delete.log', maxBytes=20*1024*1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


hour = timedelta(hours=1)

query_workflow = """
    select workflow_id from workflow
    where create_time >= '%s' and create_time < '%s'
"""
delete_result = """
    delete from result where workflow_id_id = '%s'
"""
delete_workflow = """
    delete from workflow
    where workflow_id = '%s'
"""

def delete_expired_data(start,end):
    with db.connect() as conn:
        current_time = start
        while current_time < end:
            next_time = current_time + hour
            
            workflows = conn.execute(query_workflow %(current_time, next_time)).fetchall()
            workflow_id_list = [row[0] for row in workflows]
            print(f'Deleting data from {current_time} to {next_time}, containing {len(workflow_id_list)} workflows')
            logger.info(f'Deleting data from {current_time} to {next_time}, containing {len(workflow_id_list)} workflows')
            for workflow_id in workflow_id_list:
                logger.info(f'Deleting workflow {workflow_id}')
                conn.execute(delete_result % workflow_id)
                conn.execute(delete_workflow % workflow_id)
                # print(delete_result % workflow_id)
                # print(delete_workflow % workflow_id)

            current_time = next_time
        
       
START,END = datetime(2023,8,1),datetime(2023,11,1)
delete_expired_data(START, END)