import os
import re
import json
import pyspark
import shutil
import glob
from pyspark.sql.functions import explode
from json.decoder import JSONDecodeError


def ini_spark():
    "Initializes spark session"
    spark = (
        pyspark.sql.SparkSession.builder.appName("Decode_json_files")
        .config("spark.jars")
        .getOrCreate()
    )
    return spark


def get_dirs(directory="archive"):
    "Returns a list of directory paths where financial json files are stored"
    dir_list = []
    dir_list += [
        os.path.join(directory, file)
        for file in os.listdir(directory)
        if os.path.isdir(os.path.join(directory, file))
    ]
    dir_list.sort()
    return dir_list


def get_json_in_folder(folder):
    "Returns a list of jsons files which are in a particular folder"
    json_files = []
    json_files += [os.path.join(folder, file) for file in os.listdir(folder)]
    return json_files


def get_year_qtr_of_dir(filedir):
    "Returns year and quarter of a given filedir"
    year = int(re.search("archive/(.*).QTR", filedir).group(1))
    qtr = int(re.search("QTR(.*)", filedir).group(1))
    return year, qtr


def read_bs(df):
    "Reads balance sheet from spark dataframe object"
    df_bs = df.withColumn("bs", explode("data.bs")).select("bs")
    df_bs = df_bs.select("bs.concept", "bs.label", "bs.unit", "bs.value").distinct()
    return df_bs


def read_cf(df):
    "Reads cash flow from spark dataframe object"
    df_cf = df.withColumn("bs", explode("data.cf")).select("cf")
    df_cf = df_cf.select("cf.concept", "cf.label", "cf.unit", "cf.value").distinct()
    return df_cf


def read_ic(df):
    "Reads income statement from spark dataframe object"
    df_ic = df.withColumn("bs", explode("data.cf")).select("ic")
    df_ic = df_ic.select("ic.concept", "ic.label", "ic.unit", "ic.value").distinct()
    return df_ic


def read_ticker_in_json(json_file):
    "Returns spark dataframe containing ticker symbol from json file"
    try:
        df = spark.read.option("multiline", "true").json(json_file)
        return df.select("symbol").collect()[0][0]
    except NameError:
        print('Please initialize Spark session in variable called "spark"')


def get_year_qtr_of_dir(filedir):
    year = int(re.search("archive/(.*).QTR", filedir).group(1))
    qtr = int(re.search("QTR(.*)", filedir).group(1))
    return year, qtr


def main():
    df = spark.read.option("multiline", "true").json(json_file)
    return df


def get_ticker_in_json(json_file):
    try:
        with open(json_file, "r") as jf:
            df = json.load(jf)
        return df["symbol"]
    except (IsADirectoryError, JSONDecodeError):
        pass


def dump_summary():
    dir_list = get_dirs()
    spark = ini_spark()
    for quarter_folder in dir_list:
        json_files = get_json_in_folder(quarter_folder)
        collect_df = []
        for json_id in range(len(json_files)):
            json_file = json_files[json_id]
            ticker = get_ticker_in_json(json_file)
            year, quarter = get_year_qtr_of_dir(quarter_folder)
            collect_df.append(
                {"json_id": json_id, "ticker": ticker, "year": year, "quarter": quarter}
            )
        df = spark.createDataFrame(collect_df)
        print(df.show())
        df.write.csv(f"data/summary_df_{year}_{quarter}.csv")


def read_summary(remove_aux_dirs=False):
    dir_list = get_dirs()
    spark = ini_spark()
    df = None
    for quarter_folder in dir_list:
        year, quarter = get_year_qtr_of_dir(quarter_folder)
        if df is None:
            df = spark.read.csv(f"data/summary_df_{year}_{quarter}.csv")
            # print(df.show())
        else:
            tmp_df = spark.read.csv(f"data/summary_df_{year}_{quarter}.csv")
            df = df.union(tmp_df)
    df.write.csv(f"data/summary.csv")
    if remove_aux_dirs == True:
        [shutil.rmtree(path) for path in glob.glob("data/summary_df_*")]
    return df


if __name__ == "__main__":
    dump_sum = True
    if dump_sum == True:
        dump_summary()
    else:
        pass
    df = read_summary(remove_aux_dirs=True)
    print(df.show())

    # parquet_files = get_dirs(directory='data')
    # spark = ini_spark()
    # mergedDF = spark.read.option("mergeSchema", "true").parquet(parquet_files[0])
    # print(mergedDF.count())
