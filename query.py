from typing import cast

from ppl_datalake import DatalakeClient
from ppl_datalake.impl_iceberg import TableIceberg

if __name__ == '__main__':
    client = DatalakeClient('c5e8791b-3ab1-4e2b-98ea-458460c95d52')
    table = cast(TableIceberg, client.get_namespace('ars', group='pilot').get_table('results'))
     
    for doc in table.query_by_sql('select * from ars.results limit 10'):
        print(doc)