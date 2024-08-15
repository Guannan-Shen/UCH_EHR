from typing import List

import pandas as pd
import jellyfish
import difflib
import distance
import Levenshtein
import os


# return type pd.DataFrame vs pd.Series
def str_similarity(df_use: pd.DataFrame, two_cols: List, similarity_type: str) -> pd.Series:
    dist_list = ["Levenshtein", "Damerau–Levenshtein", "Jaro", "Jaro-Winkler", "SeqMatcher", "Jaccard"]
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
    if similarity_type == "Jaccard":
        series = df_use.apply(lambda x: 1 - distance.jaccard(x[two_cols[0]], x[two_cols[1]]), axis=1)
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
    current_wd = os.getcwd()
    save_path = os.path.join(current_wd, "example_str_similar.csv")
    df.to_csv(save_path)
