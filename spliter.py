import argparse
import zipfile
from joblib import Parallel, delayed
import pandas as pd
import dask.dataframe as dd


def split(day: str):
    df: pd.read_csv = dd.read_csv("data/StandardEquities_{}_out.csv".format(day),
                                  dtype={"情報種別":object,"取引所種別":object,"マルチキャストグループ":object,
                                         "銘柄コード":object,"日付":object,"時刻":object,"アップデート種別":object,
                                         "約定値段":object,"約定数量":object,"寄り前/場間":object,"売成行数量":object,
                                         "売　気配１（価格）":object,"売　気配１（数量）":object,"売　気配２（価格）":object,
                                         "売　気配２（数量）":object,"売　気配３（価格）":object,"売　気配３（数量）":object,
                                         "売　気配４（価格）":object,"売　気配４（数量）":object,"売　気配５（価格）":object,
                                         "売　気配５（数量）":object,"売　気配６（価格）":object,"売　気配６（数量）":object,
                                         "売　気配７（価格）":object,"売　気配７（数量）":object,"売　気配８（価格）":object,
                                         "売　気配８（数量）":object,"売　ＯＶＥＲ（数量）":object,"買成行数量":object,
                                         "買　気配１（価格）":object,"買　気配１（数量）":object,"買　気配２（価格）":object,
                                         "買　気配２（数量）":object,"買　気配３（価格）":object,"買　気配３（数量）":object,
                                         "買　気配４（価格）":object,"買　気配４（数量）":object,"買　気配５（価格）":object,
                                         "買　気配５（数量）":object,"買　気配６（価格）":object,"買　気配６（数量）":object,
                                         "買　気配７（価格）":object,"買　気配７（数量）":object,"買　気配８（価格）":object,
                                         "買　気配８（数量）":object,"買　ＵＮＤＥＲ（数量）":object}).compute()
    zip_file: zipfile.ZipFile = zipfile.ZipFile("save/{}.zip".format(day), mode="w")

    def zip_save(i_ticker, group_df: pd.DataFrame, zip: zipfile.ZipFile):
        group_df.to_csv(zip.write("{}.csv".format(i_ticker)))
    Parallel(n_jobs=-1, verbose=10)([delayed(zip_save)(i_ticker=i_ticker, group_df=group, zip=zip_file)
                                     for i_ticker, group in df.groupby("銘柄コード")])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('day', help='processing day')
    args = parser.parse_args()
    split(day=args.day)
