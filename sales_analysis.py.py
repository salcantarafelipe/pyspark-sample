from pyspark.sql.functions import lit, col, coalesce, sum
from pyspark.sql import DataFrame, SparkSession
from functools import reduce


spark = (
    SparkSession.builder
    .appName("VIP Sales Analysis")
    .getOrCreate()
    )


df_vips1 = (
    spark
    .read
    .format('csv')
    .options(header=True,inferSchema=True)
    .load('/path/to/local/vips_2020-11-01.csv')
    .filter(col('country') == 'The Netherlands')
    .select(
        'vip_id',
        'first_name',
        'last_name',
        'email'
    )

    )
df_vips2 = (
    spark
    .read
    .format('csv')
    .options(header=True,inferSchema=True)
    .load('/path/to/local/vips_2020-11-15.csv')
    .filter(col('country') == 'The Netherlands')
    .select(
        'vip_id',
        'first_name',
        'last_name',
        'email'
    )
    )
df_vips3 = (
    spark
    .read
    .format('csv')
    .options(header=True,inferSchema=True)
    .load('/path/to/local/vips_2020-11-25.csv')
    .filter(col('country') == 'The Netherlands')
    .select(
        'vip_id',
        'first_name',
        'last_name',
        'email'
    )
            )

df_vip_to_profile = (
    spark
    .read
    .format('csv')
    .options(header=True,inferSchema=True, delimiter=';')
    .load('/path/to/local/umd_vip_to_profile_mapping.csv')
    .filter(col('active') == 'yes')
    )

df_transaction = spark.read.format('parquet').options(header=True,inferSchema=True).load('/path/to/local/transactions.parquet')


list_of_dfs = [df_vips1, df_vips2, df_vips3]
vips = reduce(DataFrame.unionByName, list_of_dfs)


target_vip = (
    vips
    .join(
        df_vip_to_profile, 
        on=['vip_id']
    )
    .select(
        'first_name',
        'last_name',
        'email',
        'profile_id'
    )
    .dropDuplicates('profile_id')
)


result = (
    target_vip
    .join(df_transaction, on=['profile_id'], how='left')
    .withColumn('sales_revenue', (col('recommended_retail_price_per_unit') - coalesce('discount_amount_per_unit', lit(0))) * col('quantity'))
    .groupBy('first_name', 'last_name', 'email')
    .agg(sum('sales_revenue').alias('total_sales'))
    .fillna(0, subset=['total_sales'])
    )


result.show()
spark.stop()
