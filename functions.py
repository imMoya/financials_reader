import pyspark


def ini_spark():
    "Initializes spark session"
    spark = (
        pyspark.sql.SparkSession.builder.appName("Decode_json_files")
        .config("spark.jars")
        .getOrCreate()
    )
    return spark
