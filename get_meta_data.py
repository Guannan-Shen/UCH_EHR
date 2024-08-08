from typing import Optional, List, Dict

import dask.dataframe as dd
import pandas as pd
import glob
import os
import gc


def get_shape_cols(folder_path: str, extension: str, save_path: Optional[str] = None) -> None:
    """
    Reads all "extension" type files in the specified folder using Dask, transforms them to Pandas DataFrames,
    and exports the shape and column names of all DataFrames to a single CSV file .

    Parameters:
    folder_path (str): The path to the folder containing the .csv files.
    extension: such as "*.csv", TODO: now only works for csv
    save_path: by default the working directory
    """
    default_types = ["*.csv"]
    files = glob.glob(os.path.join(folder_path, extension))
    if not files:
        print("No such type files found in the given folder.")
        return

    # for all dask read_csv using dtype='object', to avoid errors, but it is memory costly
    if extension not in default_types:
        print(f"Only support listed data file types: {default_types}")
        return
    # shape and column names are metadata
    metadata = []
    if extension == "*.csv":
        for file in files:
            try:
                df = dd.read_csv(file, dtype="object", low_memory=False).compute()
                metadata.append({
                    'file': os.path.basename(file),
                    'rows': df.shape[0],
                    'columns': df.shape[1],
                    'column_names': list(df.columns)
                })
                # to unload large dataset from the memory
                del df
                gc.collect()
            except Exception as e:
                print(f"Error reading {file}: {e}")
                continue
    # Create a DataFrame from the metadata
    metadata_df = pd.DataFrame(metadata)

    if not save_path:
        current_wd = os.getcwd()
        # if no saving path provided, using the current working directory
        metadata_out_path = os.path.join(current_wd, "shapes_cols.csv")
        metadata_df.to_csv(metadata_out_path)


if __name__ == "__main__":
    print(os.getcwd())
    f_path = "//data/dept/SOM/ACCORDS/PiFolders/PI_Colborn/UCHealthSOR/Data/C2730_20240628"
    get_shape_cols(f_path, "*.csv")
