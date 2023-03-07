import os
import finance_reader


if __name__ == "__main__":
    fr = finance_reader.FolderReader(directory="../archive", csv_directory="data")
    if not os.path.exists("data/"):
        fr.dump_summary(unique_df=True, remove_aux_dirs=False)
    # sm = finance_spark.StockHistory(df_loc="data/summary.csv")
    # df = sm.unique_stock(ticker="AAPL")
    # print(df.show())
