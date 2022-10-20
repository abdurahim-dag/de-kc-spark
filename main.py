import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

# поля справочника
dim_columns = ['id', 'name']

# vendor_rows = [
#     (1, 'Creative Mobile Technologies, LLC'),
#     (2, 'VeriFone Inc'),
# ]
#
# rates_rows = [
#     (1, 'Standard rate'),
#     (2, 'JFK'),
#     (3, 'Newark'),
#     (4, 'Nassau or Westchester'),
#     (5, 'Negotiated fare'),
#     (6, 'Group ride'),
# ]
#
# payment_rows = [
#     (1, 'Credit card'),
#     (2, 'Cash'),
#     (3, 'No charge'),
#     (4, 'Dispute'),
#     (5, 'Unknown'),
#     (6, 'Voided trip'),
# ]

trips_schema = StructType([
    StructField('vendor_id', StringType(), True),
    StructField('tpep_pickup_datetime', TimestampType(), True),
    StructField('tpep_dropoff_datetime', TimestampType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('ratecode_id', IntegerType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('pulocation_id', IntegerType(), True),
    StructField('dolocation_id', IntegerType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType()),
])


def agg_calc(spark: SparkSession) -> DataFrame:
    data_path = os.path.join(Path(__name__).parent, './data', '*.csv')

    trip_fact = spark.read \
        .option("delimiter",",") \
        .option("header", "true") \
        .schema(trips_schema) \
        .csv(data_path)

    datamart = trip_fact \
        .where(trip_fact['vendor_id'].isNotNull()) \
        .groupBy(trip_fact['payment_type'],
                 f.to_date(trip_fact['tpep_pickup_datetime']).alias('dt')
                 ) \
        .agg(f.avg(trip_fact['trip_distance']).alias("Avg trip km cost")) \
        .select(f.col('dt'),
                f.col('payment_type'),
                f.col('Avg trip km cost'))\
        .orderBy(f.col('dt').desc())

    return datamart

def main(spark: SparkSession):

    datamart = agg_calc(spark).cache()
    datamart.show(truncate=False, n=100)

    # joined_datamart = datamart \
    #     .join(other=vendor_dim, on=vendor_dim['id'] == f.col('vendor_id'), how='inner') \
    #     .join(other=payment_dim, on=payment_dim['id'] == f.col('payment_type'), how='inner') \
    #     .join(other=rates_dim, on=rates_dim['id'] == f.col('ratecode_id'), how='inner') \
    #     .select(f.col('dt'),
    #             f.col('vendor_id'), f.col('payment_type'), f.col('ratecode_id'), f.col('sum_amount'),
    #             f.col('avg_tips'),
    #             rates_dim['name'].alias('rate_name'), vendor_dim['name'].alias('vendor_name'),
    #             payment_dim['name'].alias('payment_name'),
    #             )

    # joined_datamart.show(truncate=False, n=100000)

    # joined_datamart.write.mode('overwrite').csv('output')



    print('end')


if __name__ == '__main__':
    main(SparkSession
         .builder
         .master("local")
         .appName('My first spark job')
         .getOrCreate())

# .config("spark.jars", "./practice4/jars/mysql-connector-java-8.0.25.jar")
