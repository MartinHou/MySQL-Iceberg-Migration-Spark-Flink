from pyspark.sql import SparkSession
from pyspark import SparkConf
from ppl_datalake import DatalakeClient
from datetime import datetime,timedelta
from util.db_util import get_engine
import logging
from logging.handlers import RotatingFileHandler
import time
import pandas as pd


def set_df_columns_nullable(spark: SparkSession, df, column_list, nullable=True):
    for struct_field in df.schema:
        if struct_field.name in column_list:
            struct_field.nullable = nullable
    df_mod = spark.createDataFrame(df.rdd, df.schema)
    return df_mod


if __name__ == '__main__':
    logger = logging.getLogger('spark')
    logger.setLevel('INFO')
    handler = RotatingFileHandler('spark_test.log', maxBytes=20*1024*1024, backupCount=1)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    db = get_engine('etc/staging_mysql.conf')
    # 初始化 spark 会话
    client = DatalakeClient(token='c5e8791b-3ab1-4e2b-98ea-458460c95d52')
    ns = client.get_namespace('ars', group='pilot')
    tos_info = ns.ns_info['connect_info']['tos']
    conf = (
        SparkConf()
            .setAppName("test")
            .setMaster("local[*]")
            .set("spark.driver.extraClassPath", "mysql-connector-j-8.1.0.jar")  # 设置 JDBC 驱动路径
    )
    
    spark = (
        SparkSession
            .builder
            .config(conf=conf)
            .config("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.hive.type", "HIVE")
            .config("spark.sql.catalog.hive.uri", "thrift://100.68.81.171:9083")
            .config("fs.tos.impl", "com.volcengine.cloudfs.fs.TosFileSystem")
            .config("fs.AbstractFileSystem.tos.impl", "com.volcengine.cloudfs.fs.TOS")
            .config("fs.tos.endpoint", tos_info['endpoint'])
            .config("fs.tos.ssl.channel.mode", "default_jsse_with_gcm")
            .config("fs.tos.access.key", tos_info['access_key'])
            .config("fs.tos.secret.key", tos_info['secret_key'])
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.session.timeZone", "Asia/Shanghai")
            # .config('spark.sql.iceberg.handle-timestamp-without-timezone', 'true')
    )
    spark = spark.getOrCreate()
    
    url = "jdbc:mysql://10.10.2.244:3306/ars_test"
    driver = "com.mysql.jdbc.Driver"
    # table = "result"
    properties = {
        "user": "ars_prod",
        "password": "01234567"
    }
    
    output_table = 'result_test'
    sql = """
        (
            SELECT 
                t2.*
            FROM workflow t1 JOIN result t2 ON t2.workflow_id_id = t1.workflow_id
            WHERE t1.create_time >= '%s' AND t1.create_time < '%s'
        ) AS t
    """
    batch_sql = """
        (
            SELECT 
                t2.*
            FROM workflow t1 JOIN result t2 ON t2.workflow_id_id = t1.workflow_id
            WHERE t1.create_time >= '%s' AND t1.create_time < '%s'
            LIMIT %d OFFSET %d
        ) AS t
    """
    check_cnt_sql = """
        SELECT 
            COUNT(*)
        FROM workflow t1 JOIN result t2 on t2.workflow_id_id = t1.workflow_id
        WHERE t1.create_time >= '%s' AND t1.create_time < '%s'
    """
    BATCH_SIZE = 5000
    def migrate_day_results(start_dt: datetime, end_dt: datetime):
        for i in range(2):
            try:
                results_cnt = conn.execute(check_cnt_sql % (start_dt, end_dt)).scalar()
                break
            except Exception as e:
                print(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据,get cnt error. Error: {e}')
                logger.error(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据,get cnt error. Error: {e}')
                time.sleep(1)
        if not results_cnt or results_cnt == 0:
            print(f'{start_dt} 到 {end_dt} 无数据')
            logger.info(f'{start_dt} 到 {end_dt} 无数据')
            return
        if results_cnt>BATCH_SIZE:
            if (end_dt-start_dt).total_seconds() <= 2:
                offset = 0
                while offset < results_cnt:
                    print(f'迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条，已迁移 {offset} 条')
                    logger.info(f'迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条，已迁移 {offset} 条')
                    final_sql = batch_sql % (start_dt, end_dt,BATCH_SIZE,offset)
                    for i in range(2):
                        try:
                            df = spark.read.jdbc(url=url, table=final_sql, properties=properties)
                            df = set_df_columns_nullable(spark,df,['id','create_time'],nullable=False)
                            # df = df.withColumn("create_time", F.from_utc_timestamp(df["create_time"], "Asia/Shanghai"))
                            # df = df.withColumn("create_time", F.from_utc_timestamp(df["update_time"], "Asia/Shanghai"))
                            break
                        except Exception as e:
                            print(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条，已迁移 {offset} 条. Read error: {e}')
                            logger.error(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条，已迁移 {offset} 条. Read error: {e}')
                            time.sleep(1)
                    else:
                        raise Exception(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条，已迁移 {offset} 条. Read error')
                    for i in range(2):
                        try:
                            df.write.format("iceberg").mode("append").saveAsTable("hive.ars.%s" % output_table)
                            break
                        except Exception as e:
                            print(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条，已迁移 {offset} 条. Write error: {e}')
                            logger.error(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条，已迁移 {offset} 条. Write error: {e}')
                            time.sleep(1)
                    else:
                        raise Exception(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条，已迁移 {offset} 条. Write error')
                    try:
                        df.unpersist(blocking=True)
                    except Exception as e:
                        print(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条，已迁移 {offset} 条. Unpersist error: {e}')
                        logger.error(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条，已迁移 {offset} 条. Unpersist error: {e}')
                        raise Exception(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条，已迁移 {offset} 条. Unpersist error: {e}')
                    offset += BATCH_SIZE
            else:
                diff_in_seconds = (end_dt - start_dt).total_seconds()
                mid = start_dt + timedelta(seconds=diff_in_seconds//2)
                migrate_day_results(start_dt, mid)
                migrate_day_results(mid, end_dt)
        else:
            print(f'迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条')
            logger.info(f'迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条')
            for i in range(2):
                try:
                    df = spark.read.jdbc(url=url, table=sql % (start_dt, end_dt), properties=properties)
                    df = set_df_columns_nullable(spark,df,['id','create_time'],nullable=False)
                    # df = df.withColumn("create_time", F.from_utc_timestamp(df["create_time"], "Asia/Shanghai"))
                    # df = df.withColumn("create_time", F.from_utc_timestamp(df["update_time"], "Asia/Shanghai"))
                    break
                except Exception as e:
                    print(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条. Read error: {e}')
                    logger.error(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条. Read error: {e}')
                    time.sleep(1)
            else:
                raise Exception(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条. Read error')
            for i in range(2):  # Retry up to 5 times
                try:
                    df.write.format("iceberg").mode("append").saveAsTable("hive.ars.%s" % output_table)
                    break
                except Exception as e:
                    print(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条. Write error: {e}')
                    logger.error(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条. Write error: {e}')
                    time.sleep(1)
            else:
                raise Exception(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条. Write error')
            try:
                df.unpersist(blocking=True)
            except Exception as e:
                print(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条，已迁移 {offset} 条. Unpersist error: {e}')
                logger.error(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条，已迁移 {offset} 条. Unpersist error: {e}')
                raise Exception(f'#{i}:迁移 {start_dt} 到 {end_dt} 的数据，共 {results_cnt} 条，已迁移 {offset} 条. Unpersist error: {e}')
            
    START_DATE = datetime(2022,4,5)
    END_DATE = datetime(2023,12,10)
    with db.connect() as conn:
        for date in pd.date_range(START_DATE,END_DATE):
            for h in range(24):
                start = date + timedelta(hours=h)
                end = date + timedelta(hours=h+1)
                try:
                    migrate_day_results(start, end)
                except Exception as e:
                    print(f'{e}')
                    logger.error(f'{e}')
        # migrate_day_results(datetime(2023,9,17,20,24,22), datetime(2023,9,18))
        
        # offset = 25000
        # while offset<85501:
        #     print(f'迁移 {datetime(2023,4,9,0,12,9)} 到 {datetime(2023,4,9,0,12,11)} 的数据，共 {85501} 条，已迁移 {offset} 条')
        #     final_sql = batch_sql % (datetime(2023,4,9,0,12,9), datetime(2023,4,9,0,12,11),BATCH_SIZE,offset)
        #     df = spark.read.jdbc(url=url, table=final_sql, properties=properties)
        #     df.write.format("iceberg").mode("append").saveAsTable("hive.ars.%s" % output_table)
        #     df.unpersist(blocking=True)
        #     offset += BATCH_SIZE
