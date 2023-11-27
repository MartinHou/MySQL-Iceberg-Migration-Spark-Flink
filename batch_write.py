import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from util.db_util import get_engine
from datetime import datetime, timedelta

    
if __name__=='__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1).enable_checkpointing(3000)
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
            'url' = 'jdbc:mysql://xxx',
            'table-name' = 'result',
            'username' = 'ars_dev',
            'password' = '01234567'
        );
    """)
    
    # table_env.execute_sql("""
    #     CREATE TABLE src_workflow (
    #         workflow_id STRING,
    #         workflow_type STRING, 
    #         workflow_name STRING, 
    #         `user` STRING, 
    #         workflow_input STRING, 
    #         workflow_output STRING, 
    #         log STRING, 
    #         workflow_status STRING, 
    #         priority INT, 
    #         tag STRING, 
    #         create_time TIMESTAMP(6), 
    #         update_time TIMESTAMP(6), 
    #         batch_id_id STRING, 
    #         hook STRING, 
    #         device STRING, 
    #         tos_id STRING, 
    #         device_num INT, 
    #         data_source STRING, 
    #         category STRING, 
    #         upload_ttl DOUBLE, 
    #         bag_nums INT, 
    #         metric STRING
    #     ) WITH (
    #         'connector' = 'jdbc',
    #         'url' = 'jdbc:mysql://xxx',
    #         'table-name' = 'workflow',
    #         'username' = 'ars_prod',
    #         'password' = '01234567'
    #     )
    # """)
    
    # 创建Iceberg目录
    table_env.execute_sql("CREATE CATALOG iceberg WITH ("
                        "'type'='iceberg', "
                        "'catalog-type'='hive', "
                        "'uri'='thrift://xxx',"
                        "'warehouse'='xxx')")
    
    # table_env.execute_sql("""
    #                     SELECT * FROM src_result LIMIT 1
    #                       """).print()
    # table_env.execute_sql("""
    #                     SELECT * FROM src_workflow LIMIT 1
    #                       """).print()
    print('开始转移数据')
    # table_env.execute_sql("""
    #     INSERT INTO iceberg.ars.results
    #     SELECT 
    #         ...
    #     FROM src_workflow t1 JOIN src_result t2 on t2.workflow_id_id = t1.workflow_id
    #     WHERE t1.create_time >= '2023-07-13 00:00:00'
    #         AND t1.create_time <'2023-07-13 00:00:30';
    # """)
    
        # INSERT INTO iceberg.ars.results
    t_res = table_env.execute_sql("""
        INSERT INTO iceberg.ars.results
        SELECT 
            id,
            input_md5,
            output_md5,
            log,
            metric,
            CAST(create_time AS STRING),
            CAST(update_time AS STRING),
            workflow_id_id,
            error_details,
            error_stage,
            error_type  
        FROM src_result
    """)
    # with t_res.collect() as res:
    #     for i,row in enumerate(res):
    #         if i%100==0:
    #             print(row)
    
    
# db = get_engine('etc/prod_mysql.conf')

# def get_result(datestr: str):
#     """
#     date: str in format of %Y-%m-%d
#     """
#     dt = datetime.strptime(datestr, '%Y-%m-%d')
#     nxt_dt = dt + timedelta(days=1)
#     nxt_dtstr = nxt_dt.strftime('%Y-%m-%d')
#     sql_pod = f'''
#         SELECT workflow_id FROM workflow
#         WHERE create_time >= '{datestr} 00:00:00' AND create_time <= '{nxt_dtstr} 23:59:59'
#     '''
#     sql_bag = '''
#         SELECT * FROM result
#         WHERE workflow_id_id in (%s)
#     '''
#     with db.connect() as conn:
#         workflow_ids = conn.execute(sql_pod).fetchall()[:1] # TODO: remove [:1]
#         results = conn.execute(sql_bag % ','.join([f'"{x[0]}"' for x in workflow_ids])).fetchall()
#     return results[:2]