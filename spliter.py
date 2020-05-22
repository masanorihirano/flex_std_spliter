import argparse
import os
import shutil
from tqdm import tqdm
import pandas as pd


def split(day: str):
    if os.path.exists("save/{}".format(day)):
        shutil.rmtree("save/{}".format(day))
    os.mkdir("save/{}".format(day))
    input_file = "data/StandardEquities_{}_out.csv".format(day)
    rows = sum([1 for _ in open(input_file)])
    interval = 100000
    i_max = (rows - 1) // 100000 + 1
    header = pd.read_csv(input_file, nrows=0).columns
    for i in tqdm(range(i_max)):
        df: pd.read_csv = pd.read_csv(input_file, header=None, names=header, nrows=interval,
                                      skiprows=(1 + interval * i),
                                      dtype={"情報種別": object, "取引所種別": object, "マルチキャストグループ": object,
                                             "銘柄コード": object, "日付": object, "時刻": object, "アップデート種別": object,
                                             "約定値段": object, "約定数量": object, "寄り前/場間": object,
                                             "売成行数量": object, "売　気配１（価格）": object, "売　気配１（数量）": object,
                                             "売　気配２（価格）": object, "売　気配２（数量）": object,
                                             "売　気配３（価格）": object, "売　気配３（数量）": object,
                                             "売　気配４（価格）": object, "売　気配４（数量）": object,
                                             "売　気配５（価格）": object, "売　気配５（数量）": object,
                                             "売　気配６（価格）": object, "売　気配６（数量）": object,
                                             "売　気配７（価格）": object, "売　気配７（数量）": object,
                                             "売　気配８（価格）": object, "売　気配８（数量）": object,
                                             "売　ＯＶＥＲ（数量）": object, "買成行数量": object, "買　気配１（価格）": object,
                                             "買　気配１（数量）": object, "買　気配２（価格）": object,
                                             "買　気配２（数量）": object, "買　気配３（価格）": object,
                                             "買　気配３（数量）": object, "買　気配４（価格）": object,
                                             "買　気配４（数量）": object, "買　気配５（価格）": object,
                                             "買　気配５（数量）": object, "買　気配６（価格）": object,
                                             "買　気配６（数量）": object, "買　気配７（価格）": object,
                                             "買　気配７（数量）": object, "買　気配８（価格）": object,
                                             "買　気配８（数量）": object, "買　ＵＮＤＥＲ（数量）": object})

        for i_ticker, group_df in df.groupby("銘柄コード"):
            file_name: str = "save/{}/{}.csv".format(day, i_ticker)
            exists: bool = os.path.exists(file_name)
            group_df.to_csv(open(file_name, mode="a"), index=False, header=(not exists))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('day', help='processing day')
    args = parser.parse_args()
    split(day=args.day)
