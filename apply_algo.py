from typing import List

import pandas as pd
import dask.dataframe as dd
import jellyfish
import difflib
import distance
import Levenshtein
import os


# return type pd.DataFrame vs pd.Series
def str_similarity(df_use: pd.DataFrame, two_cols: List, similarity_type: str) -> pd.Series:
    dist_list = ["Levenshtein",
                 # "Damerau–Levenshtein",
                 "Jaro", "Jaro-Winkler", "SeqMatcher", "Jaccard"]
    series = pd.Series()
    if similarity_type not in dist_list:
        print(f"Please provide a type of string distance to calculate, limited to types: {dist_list}")
    # Levenshtein ratio,
    if similarity_type == "Levenshtein":
        series = df_use.apply(lambda x: Levenshtein.ratio(x[two_cols[0]], x[two_cols[1]]), axis=1)
    if similarity_type == "SeqMatcher":
        series = df_use.apply(lambda x: difflib.SequenceMatcher(None, x[two_cols[0]], x[two_cols[1]]).ratio(), axis=1)
    if similarity_type == "Jaccard":
        series = df_use.apply(lambda x: 1 - distance.jaccard(x[two_cols[0]], x[two_cols[1]]), axis=1)
    if similarity_type == "Jaro":
        series = df_use.apply(lambda x: jellyfish.jaro_similarity(x[two_cols[0]], x[two_cols[1]]), axis=1)
    if similarity_type == "Jaro-Winkler":
        series = df_use.apply(lambda x: jellyfish.jaro_winkler_similarity(x[two_cols[0]], x[two_cols[1]]), axis=1)
    # if similarity_type == "Jaccard":
    #     series = df_use.apply(lambda x: 1 - distance.jaccard(x[two_cols[0]], x[two_cols[1]]), axis=1)
    # naming and return
    series.name = similarity_type
    return series


def str_similarity_parallel(df_use: pd.DataFrame, dask_npartitions: int, two_cols: List, similarity_type: str) -> pd.Series:
    """
    Using dask to speed up, 30 is a suitable number of partitions if you have 16 cores,
    now using 20 for 12 logic cores.
    For this function, calculation of string similarity, dask set return meta value as int32
    "https://stackoverflow.com/questions/45545110/make-pandas-dataframe-apply-use-all-cores"
    :param dask_npartitions:
    :param df_use:
    :param two_cols:
    :param similarity_type:
    :return:
    """
    ddf = dd.from_pandas(df_use, npartitions=dask_npartitions)
    dist_list = ["Levenshtein",
                 # "Damerau–Levenshtein",
                 "Jaro", "Jaro-Winkler", "SeqMatcher", "Jaccard"]
    series = pd.Series()
    if similarity_type not in dist_list:
        print(f"Please provide a type of string distance to calculate, limited to types: {dist_list}")
    # Levenshtein ratio,
    if similarity_type == "Levenshtein":
        # ddf_update = ddf.apply(your_func, axis=1).compute()
        series = ddf.apply(lambda row: Levenshtein.ratio(row[two_cols[0]], row[two_cols[1]]), axis=1, meta={0: 'int32'}).compute()
    if similarity_type == "SeqMatcher":
        series = ddf.apply(lambda x: difflib.SequenceMatcher(None, x[two_cols[0]], x[two_cols[1]]).ratio(), axis=1, meta={0: 'int32'}).compute()
    if similarity_type == "Jaccard":
        series = ddf.apply(lambda x: 1 - distance.jaccard(x[two_cols[0]], x[two_cols[1]]), axis=1, meta={0: 'int32'}).compute()
    if similarity_type == "Jaro":
        series = ddf.apply(lambda x: jellyfish.jaro_similarity(x[two_cols[0]], x[two_cols[1]]), axis=1, meta={0: 'int32'}).compute()
    if similarity_type == "Jaro-Winkler":
        series = ddf.apply(lambda x: jellyfish.jaro_winkler_similarity(x[two_cols[0]], x[two_cols[1]]), axis=1, meta={0: 'int32'}).compute()
    # if similarity_type == "Jaccard":
    #     series = df_use.apply(lambda row: 1 - distance.jaccard(row[two_cols[0]], row[two_cols[1]]), axis=1)
    # naming and return
    series.name = similarity_type
    return series


if __name__ == "__main__":
    data = {
        'Column1': ['apple', 'banana', 'cherry', 'date', 'elderberry', 'not the same'],
        'Column2': ['appl', 'bananna', 'cheri', 'dat', 'elderberry', 'different']
    }
    df = pd.DataFrame(data)
    dist_used_list = ["Levenshtein", "SeqMatcher", "Jaro", "Jaro-Winkler"]
    for dist in dist_used_list:
        df[f'{dist}'] = str_similarity(df, ['Column1', 'Column2'], dist)
    print(df)

    # current_wd = os.getcwd()
    # save_path = os.path.join(current_wd, "example_str_similar.csv")
    # df.to_csv(save_path)
    # test generate mean, and 2.5% and 97.5% quantiles
    result_df = pd.DataFrame()
    for dist in dist_used_list:
        temp_series = str_similarity(df, ['Column1', 'Column2'], dist)
        result_df[f'{dist}'] = pd.Series({
            'Mean': temp_series.mean(),
            '2.5% Quantile': temp_series.quantile(0.025),
            '97.5% Quantile': temp_series.quantile(0.975)
        })

    print(result_df)

    # test dask parallel
    result_df_para = pd.DataFrame()
    for dist in dist_used_list:
        temp_series = str_similarity_parallel(df, 20, ['Column1', 'Column2'], dist)
        result_df_para[f'{dist}'] = pd.Series({
            'Mean': temp_series.mean(),
            '2.5% Quantile': temp_series.quantile(0.025),
            '97.5% Quantile': temp_series.quantile(0.975)
        })

    print(result_df_para)
