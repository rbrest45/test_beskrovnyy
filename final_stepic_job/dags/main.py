from airflow import DAG
from airflow.operators.python import PythonOperator

from pyspark.sql.functions import col, regexp_replace, avg, count, lit, min, max, length, concat, substring, coalesce
from pyspark.sql import SparkSession
from datetime import datetime



def run_spark_job():
    spark = SparkSession.builder \
        .appName("DAG_for_job_stepik_8") \
        .config("spark.jars", "/opt/airflow/jars/clickhouse-jdbc-0.4.6.jar") \
        .master("local") \
        .getOrCreate()

    jdbc_url = "jdbc:clickhouse://172.18.0.9:8123/default"
    connection_properties = {
        "user": "default",  # Имя пользователя ClickHouse (по умолчанию "default")
        "password": "",  # Пароль (по умолчанию пустой)
        "driver": "com.clickhouse.jdbc.ClickHouseDriver"
    }

    df = (spark.read #Читаем csv из папки tmp монтированной в Docker
          .option("header", True)
          .option("delimiter", ",")
          .option("encoding", "UTF-16")
          .option("multiLine", "true")
          .option("inferSchema", True)
          .csv("/opt/airflow/tmp/russian_houses.csv"))

    df = df.dropna() #удаляем пустые строки

    print(f"Количество строк загруженных из файла {df.count()}")

    df_casttype = df.select( # преобразуем df под нужные типы полей, убираем null значения
        col("house_id").cast("int"),
        col("latitude").cast("float"),
        col("longitude").cast("float"),
        coalesce(col("maintenance_year").cast("int"), lit(0)).alias("maintenance_year"),
        coalesce(regexp_replace(col("square"), " ", "").cast("float"), lit(0)).alias("square"),
        coalesce(col("population").cast("int"), lit(0)).alias("population"),
        col("region"),
        col("locality_name"),
        col("address"),
        col("full_address"),
        coalesce(col("communal_service_id").cast("int"), lit(0)).alias("communal_service_id"),
        col("description"),
    )

    df_casttype.printSchema() #схема df после преобразования


    avg_year = df_casttype.filter(col("maintenance_year").between(1901, 2024)).select(avg("maintenance_year")).collect()[0][0]
    median_year = df_casttype.approxQuantile("maintenance_year", [0.5], 0.01)[0]

    print(f"Средний год постройки {round(avg_year, 0)}, медианный год постройки {median_year}")

    df_top_obj = df_casttype.groupby(col("region"), col("locality_name")) \
    .agg(count(col("region")).alias("count_obj"))

    print("Топ-10 областей и городов с наибольшим количеством объектов")
    df_top_obj.orderBy(col("count_obj").desc()).show(10, truncate=False)

    df_minmax_square_tmp = (df_casttype.filter(col("square") > 1).groupby(col("region")) \
    .agg(min(col("square")).alias("min_square"), max(col("square")).alias("max_square")))

    df_minmax_square = df_casttype.alias("a") \
    .join(df_minmax_square_tmp.alias("b"), (col("a.region") == col("b.region")) & (col("a.square") == col("b.min_square"))) \
    .select(col("a.region"), col("locality_name"), col("address"), col("a.square"), lit("Минимальная площадь").alias("square_flg")) \
    .union(
        df_casttype.alias("a") \
            .join(df_minmax_square_tmp.alias("b"),
                  (col("a.region") == col("b.region")) & (col("a.square") == col("b.max_square"))) \
            .select(col("a.region"), col("locality_name"), col("address"), col("a.square"),
                    lit("Максимальная площадь").alias("square_flg")))

    print("Здания с максимальной и минимальной площадью в рамках каждой области")
    df_minmax_square.show(500, truncate=False)

    df_year_col = df.filter(length(col("maintenance_year").cast("string")) == 4) \
    .withColumn("year_tmp", concat(substring(col("maintenance_year").cast("string"), 1, 3),lit("0"))) \
    .groupby(col("year_tmp")).agg(count(col("region")).alias("count_obj"))

    print("Количество зданий по десятилетиям")
    df_year_col.orderBy(col("year_tmp")).show(100, truncate=False)

    df_casttype.write.jdbc(url=jdbc_url, table="default.city_house", mode="append", properties=connection_properties)
    print("Данные успешно записаны в таблицу city_house")

    df_ch = spark.read.jdbc(url=jdbc_url, table="city_house", properties=connection_properties)

    print("Данные из ClickHouse, топ 25 домов, у которых площадь больше 60 кв.м")
    df_ch.filter(col("square") > 1).select(col("address"), col("square")).orderBy(col("square").desc()).show(25, truncate=False)

    spark.stop()

dag = DAG(
    dag_id='main',
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,
    catchup=False,
)

run_pyspark_task = PythonOperator(
    task_id="run_pyspark_task",
    python_callable=run_spark_job,
    dag=dag
)

run_pyspark_task