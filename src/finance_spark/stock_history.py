from .functions import ini_spark


class StockHistory:
    def __init__(self, df_loc="data/summary.csv"):
        self.df_loc = df_loc

    def unique_stock(self, ticker="AAPL"):
        spark = ini_spark()
        df = spark.read.option("header", "true").csv(self.df_loc)
        df = df.filter(df.ticker == ticker)
        df = df.sort("year", "quarter")
        return df
