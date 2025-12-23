import dlt
from pyspark.sql.functions import *

rules = {
    'rule1' : 'product_id is not null',
    'rule2' : 'product_name is not null'
}

@dlt.table()
@dlt.expect_all(rules)
def products_staging():
    df = spark.readStream.table('dbx_cata.silver.products')
    df = df.withColumn('updated_date', to_date(current_date()))
    return df


@dlt.view()
def products_view():
    df = spark.readStream.table('live.products_staging')
    return df

dlt.create_streaming_table('products')

dlt.apply_changes(
    target = 'products',
    source = 'products_view',
    keys = ['product_id'],
    sequence_by = col('updated_date'),
    stored_as_scd_type = 2,
    except_column_list = ['updated_date']
    )
