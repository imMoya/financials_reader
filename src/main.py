import os
import FinanceSpark


if __name__ == "__main__":
    fr = FinanceSpark.FolderReader()
    if not os.path.exists(".data/"):
        fr.dump_summary()
    fr.read_summary(remove_aux_dirs=True)
