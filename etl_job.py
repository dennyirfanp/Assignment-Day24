from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Spark PostgreSQL ETL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.20") \
        .getOrCreate()

    # Read from PostgreSQL
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow_db") \
        .option("dbtable", "your_table") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .load()

    df.show()

    # Process & Write to PostgreSQL
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow_db") \
        .option("dbtable", "your_table_copy") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

if __name__ == "__main__":
    main()
