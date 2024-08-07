# import sys
# import os
from typing import Optional, List, Dict

# import pandas as pd
# import seaborn as sns
# import matplotlib.pyplot as plt
# low_memory=False issue
import dask.dataframe as dd
from pandas.io.formats.style import Styler


def load_csv_dask(file_path: str, drop: Optional[List] = None, dtype: Optional[Dict] = None) -> dd.DataFrame:
    if drop and dtype:
        return dd.read_csv(file_path, low_memory=False, dtype=dtype).drop(drop, axis=1).compute()
    if drop:
        return dd.read_csv(file_path, low_memory=False).drop(drop, axis=1).compute()
    if dtype:
        return dd.read_csv(file_path, low_memory=False, dtype=dtype).compute()
    return dd.read_csv(file_path, low_memory=False).compute()


def pretty_freq_df(styler: Styler) -> Styler:
    styler.set_caption("Frequency Table")
    return styler


# # explore
#
# ddf = dd.read_csv(file_path, low_memory=False)
# df = ddf.compute()
# print(df.shape)
if __name__ == "__main__":
    file_path_test = ("//data/dept/SOM/ACCORDS/PiFolders/PI_Colborn/UCHealthSOR/Data/C2730_20240628"
                      "/C2730_Table10_CUMedicineProcedure_20240223.csv")
    print(load_csv_dask(file_path_test).shape)
