import argparse
import zipfile
from joblib import Parallel, delayed
import pandas as pd
import dask.dataframe as dd


def split(day: str):
    df: pd.read_csv = pd.read_csv("data/StandardEquities_{}_out.csv".format(day))
    zip_file: zipfile.ZipFile = zipfile.ZipFile("save/{}.zip".format(day), mode="w")

    for i_ticker, group_df in df.groupby("銘柄コード"):
        group_df.to_csv(zip_file.write("{}.csv".format(i_ticker)), index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('day', help='processing day')
    args = parser.parse_args()
    split(day=args.day)
