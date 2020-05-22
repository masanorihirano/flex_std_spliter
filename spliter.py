import argparse
import zipfile
from tqdm import tqdm
import pandas as pd


def split(day: str):
    df: pd.read_csv = pd.read_csv("data/StandardEquities_{}_out.csv".format(day))
    zip_file: zipfile.ZipFile = zipfile.ZipFile("save/{}-py.zip".format(day), mode="a")

    for i in tqdm(range(len(df) // 10000 + 1)):
        for i_ticker, group_df in df[(i * 10000) : min((i+1) * 10000, len(df))].groupby("銘柄コード"):
            file_name: str = "{}/{}.csv".format(day, i_ticker)
            exists: bool = file_name in zip_file.namelist()
            group_df.to_csv(zip_file.write(), index=False, header=(not exists))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('day', help='processing day')
    args = parser.parse_args()
    split(day=args.day)
