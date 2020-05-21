import argparse
import zipfile
from joblib import Parallel, delayed
import pandas as pd
import dask.dataframe as dd


def split(day: str):
    df: pd.read_csv = pd.read_csv("data/StandardEquities_{}_out.csv".format(day))
    zip_file: zipfile.ZipFile = zipfile.ZipFile("save/{}.zip".format(day), mode="w")

    def zip_save(i_ticker, group_df: pd.DataFrame, zip: zipfile.ZipFile):
        group_df.to_csv(zip.write("{}.csv".format(i_ticker)))

    for i_ticker, group in df.groupby("銘柄コード")]):
        zip_save(i_ticker=i_ticker, group_df=group, zip=zip_file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('day', help='processing day')
    args = parser.parse_args()
    split(day=args.day)
