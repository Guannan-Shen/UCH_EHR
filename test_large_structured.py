import freq_table
import time


if __name__ == "__main__":
    # test for largest structured dataset
    # C2730_Table11_Lab_20240223
    start_time = time.time()
    file_path_large = ("//data/dept/SOM/ACCORDS/PiFolders/PI_Colborn/UCHealthSOR/Data/C2730_20240628"
                       "/C2730_Table11_Lab_20240223.csv")
    dtype = {"EpicCptCode": "object", "LabResult": "object"}
    print(freq_table.load_csv_dask(file_path_large, dtype=dtype).shape)
    print("--- %s seconds ---" % (time.time() - start_time))
