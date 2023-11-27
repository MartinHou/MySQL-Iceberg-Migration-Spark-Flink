from ppl_datalake import DatalakeClient

# 执行了 ppl_datalake login 的情况或者环境变量设置了 API Token 的情况下，客户端类会自动加载
# datalake_client = DatalakeClient()
# API Token 作为构造参数传入
datalake_client = DatalakeClient(token='c5e8791b-3ab1-4e2b-98ea-458460c95d52')


# test_iceberg是个iceberg type的namespace，只有在iceberg namespace才可以使用流式插入功能，
# 当前可用的iceberg namespace有ddld和test_iceberg，如需新建namespace在ppl服务台提问
namespace = datalake_client.get_namespace('ars')
table = namespace.get_table('results') # 改成具体的table
doc = {'a': 1}

table.put_async(doc) #插入一行数据
table.put_multiple_async([doc, doc, doc, doc, doc, doc]) #插入多行数据的list