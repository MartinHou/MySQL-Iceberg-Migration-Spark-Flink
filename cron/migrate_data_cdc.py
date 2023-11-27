from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common import RowKind, Row


def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(3000)
    
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
            error_type STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector' = 'mysql-cdc',
            'hostname' = '10.8.104.202',
            'port' = '3306',
            'username' = 'ars_dev',
            'password' = '01234567',
            'database-name' = 'ars_local',
            'table-name' = 'result'
        );
    """)
    
    # table_env.execute_sql("""
    #     CREATE TABLE src (
    #         id STRING,
    #         PRIMARY KEY (id) NOT ENFORCED
    #     ) WITH (
    #         'connector' = 'mysql-cdc',
    #         'hostname' = '10.8.104.202',
    #         'port' = '3306',
    #         'username' = 'ars_dev',
    #         'password' = '01234567',
    #         'database-name' = 'ars_local',
    #         'table-name' = 'a'
    #     );
    # """)
    
    table_env.execute_sql("CREATE CATALOG iceberg WITH ("
                      "'type'='iceberg', "
                      "'catalog-type'='hive', "
                      "'uri'='thrift://100.68.81.171:9083',"
                      "'warehouse'='tos://ddinfra-iceberg-test-tos/warehouse',"
                      "'format-version'='2')")
    
    # table_env.execute_sql("USE iceberg.ars")
    # table_env.execute_sql("DELETE FROM a WHERE id = 'b'")
    # table_env.execute_sql("INSERT INTO iceberg.ars.a SELECT * FROM src")
    # table_env.execute_sql("select * from src").print()
    def tmp(x:Row):
        return x.get_row_kind().name!=RowKind.DELETE.name
        
    change_log_table = table_env.sql_query("SELECT * FROM src_result")
    ds = table_env.to_changelog_stream(change_log_table).filter(tmp)
    filtered_table = table_env.from_changelog_stream(ds)
    
    table_env.create_temporary_view("src", filtered_table)
    
    # table_env.execute_sql("""
    #                       select * from src
    #                       """)
    table_env.execute_sql("""
                          INSERT INTO iceberg.ars.result_test
                          select * 
                          from src
                          """)
    
    # table_env.execute_sql("""
    #     CREATE TABLE iceberg.ars.martintest (
    #         `id` STRING,
    #         input_md5 STRING,
    #         output_md5 STRING,
    #         log STRING,
    #         metric STRING,
    #         create_time TIMESTAMP(6),      
    #         update_time TIMESTAMP(6),
    #         workflow_id_id STRING,
    #         error_details STRING,
    #         error_stage STRING,
    #         error_type STRING,
    #         PRIMARY KEY (`id`) NOT ENFORCED
    #     ) WITH (
    #         'connector' = 'iceberg',
    #         'catalog-name'='iceberg',
    #         'catalog-type' = 'hive',
    #         'warehouse'='tos://ddinfra-iceberg-test-tos/warehouse',
    #         'uri'='thrift://100.68.81.171:9083'
    #     );
    # """)

    
if __name__=='__main__':
    run()