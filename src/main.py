import os
import finance_spark


if __name__ == "__main__":
    fr = finance_spark.FolderReader(directory="../archive", csv_directory="datar")
    if not os.path.exists("datar/"):
        fr.dump_summary(unique_df=True, remove_aux_dirs=True)
    sm = finance_spark.StockMerge(df_loc="data/summary.csv")
    df = sm.unique_stock()
    print(df.show())
    