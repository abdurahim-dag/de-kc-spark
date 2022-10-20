from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

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
    #data_path = os.path.join(Path(__name__).parent, './data', '*.csv')
    #paths = list(Path('./data').glob('**/*.csv'))
    #data_path = [str(p) for p in paths]

    trip_fact = spark.read \
        .option("delimiter",",") \
        .option("header", "true") \
        .schema(trips_schema) \
        .csv("hdfs://rc1b-dataproc-m-ytxncbscpq5omqap.mdb.yandexcloud.net:8020/user/ubuntu/data-taxi-2020/")

    datamart = trip_fact \
        .where(trip_fact['vendor_id'].isNotNull() & trip_fact['total_amount'].isNotNull()) \
        .groupBy(trip_fact['payment_type'],
                 f.to_date(trip_fact['tpep_pickup_datetime']).alias('dt')
                 ) \
        .agg(f.sum(trip_fact['total_amount']).alias('sum_amount'),
             f.sum(trip_fact['trip_distance']).alias('sum_trip_distance'),
             f.avg(trip_fact['total_amount']).alias('Average trip cost')) \
        .select(f.col('dt'),
                f.col('payment_type'),
                f.col('sum_amount'),
                f.col('sum_trip_distance'),
                f.col('Average trip cost'),) \
        .withColumn('Avg trip cost',
                    f.col('sum_amount') / f.col('sum_trip_distance')) \
        .withColumn('Avg trip km cost',
                    f.col('Avg trip cost') * 1.61) \
        .select(f.col('dt'),
                f.col('payment_type'),
                f.col('Average trip cost'),
                f.col('sum_amount'),
                f.col('sum_trip_distance'),
                f.col('Avg trip km cost'))

    return datamart

def create_dict(spark, header, data):
    """создание словаря"""
    df = spark.createDataFrame(data=data, schema=header)
    return df

def main(spark: SparkSession):
    # поля справочника
    dim_columns = ['id', 'name']

    payment_rows = [
        (1, 'Credit card'),
        (2, 'Cash'),
        (3, 'No charge'),
        (4, 'Dispute'),
        (5, 'Unknown'),
        (6, 'Voided trip'),
    ]

    payment_dim = create_dict(spark, dim_columns, payment_rows)

    datamart = agg_calc(spark).cache()

    joined_datamart = datamart.join(other=payment_dim, on=payment_dim['id'] == f.col('payment_type'), how='inner') \
        .select(
            f.col('dt'),
            f.col('name').alias("Payment type"),
            f.col('Average trip cost'),
            f.col('sum_amount'),
            f.col('sum_trip_distance'),
            f.col('Avg trip km cost'),
        ) \
        .orderBy(f.col('dt').desc(), f.col('Payment type').asc())

      joined_datamart.coalesce(1).write.option("header","true").mode('overwrite').csv('output-res')

if __name__ == '__main__':
    main(SparkSession
         .builder
         .master("local")
         .appName('My first spark job')
         .getOrCreate())
