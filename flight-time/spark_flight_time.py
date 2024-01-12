import logging

from pyspark import SparkConf
from pyspark.sql import SparkSession

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def create_table(spark_session: SparkSession):
    dataset_path = './flight-time.csv'

    flight_time_csv_df = (spark_session.read
                          .format("parquet")
                          .load(dataset_path))

    spark_session.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark_session.catalog.setCurrentDatabase("AIRLINE_DB")

    flight_time_csv_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .saveAsTable("flight_data_tbl")

    query = """
            SELECT *
            FROM flight_data_tbl
        """

    count_df = spark_session.sql(query)
    count_df.show()

    logging.info(spark_session.catalog.listTables("AIRLINE_DB"))


if __name__ == "__main__":
    conf = SparkConf() \
        .setMaster('local') \
        .setAppName('lab4_2')

    spark = (SparkSession.builder
             .config(conf=conf)
             .getOrCreate())
    sc = spark.sparkContext

    create_table(spark)
