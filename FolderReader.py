import os
import json
import re
import shutil
from json.decoder import JSONDecodeError
from functions import ini_spark


class FolderReader:
    def __init__(self, directory="archive"):
        self.directory = directory

    def get_dirs(self):
        "Returns a list of directory paths where financial json files are stored"
        dir_list = []
        dir_list += [
            os.path.join(self.directory, file)
            for file in os.listdir(self.directory)
            if os.path.isdir(os.path.join(self.directory, file))
        ]
        dir_list.sort()
        return dir_list

    def get_json_in_folder(self, folder):
        "Returns a list of jsons files which are in a particular folder"
        json_files = []
        json_files += [os.path.join(folder, file) for file in os.listdir(folder)]
        return json_files

    def get_ticker_in_json(self, json_file):
        try:
            with open(json_file, "r") as jf:
                df = json.load(jf)
            return df["symbol"]
        except (IsADirectoryError, JSONDecodeError):
            pass

    def get_year_qtr_of_dir(self, filedir):
        year = int(re.search("archive/(.*).QTR", filedir).group(1))
        qtr = int(re.search("QTR(.*)", filedir).group(1))
        return year, qtr

    def dump_summary(self):
        dir_list = self.get_dirs()
        spark = ini_spark()
        for quarter_folder in dir_list:
            json_files = self.get_json_in_folder(quarter_folder)
            collect_df = []
            for json_id in range(len(json_files)):
                json_file = json_files[json_id]
                ticker = self.get_ticker_in_json(json_file)
                year, quarter = self.get_year_qtr_of_dir(quarter_folder)
                collect_df.append(
                    {
                        "json_id": json_id,
                        "ticker": ticker,
                        "year": year,
                        "quarter": quarter,
                    }
                )
            df = spark.createDataFrame(collect_df)
            print(df.show())
            df.write.csv(f"data/summary_df_{year}_{quarter}.csv")

    def read_summary(self, remove_aux_dirs=False):
        dir_list = self.get_dirs()
        spark = ini_spark()
        df = None
        for quarter_folder in dir_list:
            year, quarter = self.get_year_qtr_of_dir(quarter_folder)
            if df is None:
                df = spark.read.csv(f"data/summary_df_{year}_{quarter}.csv")
            else:
                tmp_df = spark.read.csv(f"data/summary_df_{year}_{quarter}.csv")
                df = df.union(tmp_df)
        df.write.csv(f"data/summary.csv")
        if remove_aux_dirs == True:
            [shutil.rmtree(path) for path in glob.glob("data/summary_df_*")]
        return df