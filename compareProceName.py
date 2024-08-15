import my_load
import pandas as pd
import dask.dataframe as dd


TO_GB = 1000000000

if __name__ == "__main__":
    # load in
    file_path_tab11 = ("//data/dept/SOM/ACCORDS/PiFolders/PI_Colborn/UCHealthSOR/Data/C2730_20240628"
                       "/C2730_Table11_Lab_20240223.csv")
    # dtype = {"EpicCptCode": "object", "LabResult": "object"}
    cols11 = ["arb_person_id", "arb_encounter_id", 'LabComponentName', 'LabPanelName']

    file_path_tab10 = ("//data/dept/SOM/ACCORDS/PiFolders/PI_Colborn/UCHealthSOR/Data/C2730_20240628"
                       "/C2730_Table10_CUMedicineProcedure_20240223.csv")
    cols10 = ["arb_person_id", "arb_encounter_id", "ProcedureCategoryName", "ProcedureName"]
    tab10 = my_load.load_csv_dask_cols(file_path_tab10, cols=cols10)
    print(tab10.shape)
    print(tab10.memory_usage(index=True).sum() / TO_GB)
    tab11 = my_load.load_csv_dask_cols(file_path_tab11, cols=cols11)
    print(tab11.shape)
    print(tab11.memory_usage(index=True).sum() / TO_GB)
    # join_tab11_10 = pd.concat([tab11, tab10], axis=1, join="inner")
    # join_tab11_10 = pd.merge(tab11, tab10, on=["arb_person_id", "arb_encounter_id"], how="inner")
    # the best method is de_duplicate and dask.dataframe.merge
    # using dask, dd.merge
    join_tab11_10 = dd.merge(tab11, tab10, on=["arb_person_id", "arb_encounter_id"], how="inner")
    print(join_tab11_10.head(10))
