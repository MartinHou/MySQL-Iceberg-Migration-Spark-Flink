from util.db_util import get_engine
from datetime import datetime, timedelta

db = get_engine('etc/prod_mysql.conf')

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
            # print(f'Deleting data from {current_time} to {next_time}, containing {len(workflow_id_list)} workflows')
            for workflow_id in workflow_id_list:
                conn.execute(delete_result % workflow_id)
                conn.execute(delete_workflow % workflow_id)

            current_time = next_time
        

def run():
    EXPIRE_DAYS = 90
    end = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0) - timedelta(days=EXPIRE_DAYS)
    start = end-timedelta(days=1)
    delete_expired_data(start, end)
    
if __name__ == "__main__":
    run()