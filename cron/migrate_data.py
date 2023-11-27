from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from datetime import datetime, timedelta

# Must ensure that at least 32 slots are available for the task
# as insert operation might fail due to lack of slots, causing data loss

ICEBERG_RESULT_TABLE = 'result_test'
ICEBERG_WORKFLOW_TABLE = 'workflow_test'

def insert_iceberg(table_env: StreamTableEnvironment, workflows):
    print(f'Inserting')
    table_env.execute_sql(f"""
                            INSERT INTO iceberg.ars.{ICEBERG_RESULT_TABLE}
                            SELECT * FROM src_result
                            where workflow_id_id in ({','.join(workflows)})
                            """)
    table_env.execute_sql(f"""
                            INSERT INTO iceberg.ars.{ICEBERG_WORKFLOW_TABLE}
                            SELECT 
                            workflow_id,
                            workflow_type, 
                            workflow_name, 
                            user, 
                            workflow_input, 
                            workflow_output, 
                            log, 
                            workflow_status, 
                            priority, 
                            tag _tag, 
                            create_time, 
                            update_time, 
                            batch_id_id, 
                            hook, 
                            device, 
                            tos_id, 
                            device_num, 
                            data_source, 
                            category, 
                            CAST(ROUND(upload_ttl) AS INT), 
                            bag_nums, 
                            metric
                            FROM src_workflow
                            where workflow_id in ({','.join(workflows)})
                            """)
    

# def delete_mysql(table_env: StreamTableEnvironment, workflows):
#     print(f'Deleting')
#     table_env.execute_sql(f"""
#                             DELETE FROM src_result
#                             where workflow_id_id in ({','.join(workflows)})
#                             """)
#     table_env.execute_sql(f"""
#                             DELETE FROM src_workflow
#                             where workflow_id in ({','.join(workflows)})
#                             """)
    

def migrate_by_time(table_env: StreamTableEnvironment, start_dt: datetime, end_dt: datetime):
    workflow_res = table_env.execute_sql(f"""
                        SELECT workflow_id, create_time FROM src_workflow
                        WHERE create_time >= '{start_dt}' and create_time < '{end_dt}'
                        """)
    workflow_cnt = 0
    workflow_cache = []
    for workflow in workflow_res.collect():
        workflow_cnt+=1
        workflow_id, create_time = workflow[0], workflow[1]
        print(f"Workflow {workflow_id} created at {datetime.strftime(create_time, '%Y-%m-%d %H:%M:%S')}")
        workflow_cache.append(f"'{workflow_id}'")
        
        if len(workflow_cache)<200:
            continue
        
        insert_iceberg(table_env, workflow_cache)
        workflow_cache.clear()
    
    if workflow_cache:
        insert_iceberg(table_env, workflow_cache)
        workflow_cache.clear()
    print(f'Migrated {workflow_cnt} workflows before {end_dt}!')
    

def run(start_date: datetime=None, end_date: datetime=None):
    """
        Tip: end_date is not included
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    
    table_env = StreamTableEnvironment.create(env)
    table_env.execute_sql("""
        CREATE TABLE src_result (
            id STRING,
            input_md5 STRING,
            output_md5 STRING,
            log STRING,
            metric STRING,
            create_time TIMESTAMP(6),
            update_time TIMESTAMP(6),
            workflow_id_id STRING,
            error_details STRING,
            error_stage STRING,
            error_type STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://10.10.2.244/ars_prod?serverTimezone=Asia/Shanghai',
            'table-name' = 'result',
            'username' = 'ars_spark',
            'password' = '01234567'
        );
    """)
    table_env.execute_sql("""
        CREATE TABLE src_workflow (
            workflow_id STRING,
            workflow_type STRING, 
            workflow_name STRING, 
            `user` STRING, 
            workflow_input STRING, 
            workflow_output STRING, 
            log STRING, 
            workflow_status STRING, 
            priority INT, 
            tag STRING, 
            create_time TIMESTAMP(6), 
            update_time TIMESTAMP(6), 
            batch_id_id STRING, 
            hook STRING, 
            device STRING, 
            tos_id STRING, 
            device_num INT, 
            data_source STRING, 
            category STRING, 
            upload_ttl DOUBLE, 
            bag_nums INT, 
            metric STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://10.10.2.244/ars_prod?serverTimezone=Asia/Shanghai',
            'table-name' = 'workflow',
            'username' = 'ars_spark',
            'password' = '01234567'
        );
    """)
    table_env.execute_sql("CREATE CATALOG iceberg WITH ("
                      "'type'='iceberg', "
                      "'catalog-type'='hive', "
                      "'uri'='thrift://100.68.81.171:9083',"
                      "'warehouse'='tos://ddinfra-iceberg-test-tos/warehouse',"
                      "'format-version'='2')")
    
    if start_date is None or end_date is None:
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=1)
        print(start_date, end_date)
        
    migrate_by_time(table_env, start_date, end_date)
    
    
if __name__=='__main__':
    # run(datetime(2023, 8, 1), datetime(2023, 8, 2))
    run()