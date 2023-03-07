"""
Reads financials dataset with pyspark
"""

from finance_reader.finance_scrapper import FinanceScrapper
from finance_reader.folder_reader import FolderReader
from finance_reader.functions import ini_spark
from finance_reader.stock_history import StockHistory
