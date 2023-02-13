from .functions import ini_spark


class StockMerge:
    def __init__(self, df_loc="data/summary.csv"):
        self.df_loc = df_loc

    def unique_stock(self, ticker="A"):
        spark = ini_spark()
        df = spark.read.option("header", "true").csv(self.df_loc)
        return df
