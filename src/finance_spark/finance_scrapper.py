from .functions import ini_spark
from pyspark.sql.functions import explode


class FinanceScrapper:
    def __init__(self, file):
        self.file = file
        spark = ini_spark()
        self.df = spark.read.option("multiline", "true").json(file)

    def read_bs(self):
        "Reads balance sheet from spark dataframe object"
        df_bs = df.withColumn("bs", explode("data.bs")).select("bs")
        df_bs = df_bs.select("bs.concept", "bs.label", "bs.unit", "bs.value").distinct()
        return df_bs

    def read_cf(self):
        "Reads cash flow from spark dataframe object"
        df_cf = self.df.withColumn("cf", explode("data.cf")).select("cf")
        df_cf = df_cf.select("cf.concept", "cf.label", "cf.unit", "cf.value").distinct()
        return df_cf

    def read_ic(self):
        "Reads income statement from spark dataframe object"
        df_ic = self.df.withColumn("ic", explode("data.ic")).select("ic")
        df_ic = df_ic.select("ic.concept", "ic.label", "ic.unit", "ic.value").distinct()
        return df_ic
