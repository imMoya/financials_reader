import os
import pandas as pd
import pyspark
from pyspark.sql.types import DataType, BooleanType, NullType, IntegerType, StringType, MapType
from pyspark.sql.functions import udf, explode

def get_dirs():
    """ Returns a list of directory paths where financial json files are stored """
    DIR = 'archive'
    dir_list = []
    dir_list += [os.path.join(DIR,file) for file in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, file))]
    dir_list.sort()
    return dir_list

def get_json_in_folder(folder):
    """ Returns a list of jsons files which are in a particular folder"""
    json_files = []
    json_files += [os.path.join(folder, file) for file in os.listdir(folder)]
    return json_files

def main():
    """
    1. Get directories where json files are located and chooses one quarter 
    2. JSON file is selected from the available ones
    3. Initialises Spark session and reads JSON file selected
    4. Defines a dataframe where Balance Sheet is stored
    
    """
    dir_list = get_dirs()
    # Get latest quarter
    quarter_folder = dir_list[-2]
    json_files = get_json_in_folder(quarter_folder)
    # Get first stock of the json files
    json_file = json_files[0]
    # Spark session
    spark = pyspark.sql.SparkSession.builder \
        .appName("Decode_json_files") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    df = spark.read.option("multiline", "true").json(json_file)
    df_bs = df.withColumn("bs", explode('data.bs')).select('bs')
    return df_bs

if __name__ == "__main__":
    main()
    
    
    
