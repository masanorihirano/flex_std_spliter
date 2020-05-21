import zipfile
from joblib import Parallel, delayed
import pandas as pd


def split(day: str):
    df: pd.read_csv = pd.read_csv("data/StandardEquities_{}_out.csv".format(day))
    zip_file: zipfile.ZipFile = zipfile.ZipFile("save/{}.zip".format(day), mode="w")

    def zip_save(i_ticker, group_df: pd.DataFrame, zip: zipfile.ZipFile):
        group_df.to_csv(zip_file.write("{}.csv".format(i_ticker)))
    Parallel(n_jobs=-1, verbose=10)([delayed(zip_file)(i_ticker=i_ticker, group_df=group, zip=zip_file)
                                     for i_ticker, group in df.groupby("銘柄コード")])


