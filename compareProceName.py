import freq_table
import pandas as pd


if __name__ == "__main__":
    # load in
    file_path_tab11 = ("//data/dept/SOM/ACCORDS/PiFolders/PI_Colborn/UCHealthSOR/Data/C2730_20240628"
                       "/C2730_Table11_Lab_20240223.csv")
    dtype = {"EpicCptCode": "object", "LabResult": "object"}

    file_path_tab10 = ("//data/dept/SOM/ACCORDS/PiFolders/PI_Colborn/UCHealthSOR/Data/C2730_20240628"
                       "/C2730_Table10_CUMedicineProcedure_20240223.csv")
    tab10 = freq_table.load_csv_dask(file_path_tab10)
    tab11 = freq_table.load_csv_dask(file_path_tab11, dtype=dtype)
    # test
    print(tab10.shape)
    print(tab11.shape)

