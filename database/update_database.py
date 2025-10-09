import sys
import os

from config.source.basicStockData import BasicStockData

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../config/source/")))
import basicStockData

class UpdateStockData(BasicStockData):
    def __init__(self):
        super().__init__()


