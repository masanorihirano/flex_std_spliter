import argparse
import io
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
    with open(input_file, mode="r") as f:
        header = pd.read_csv(io.StringIO(f.readline()), nrows=0).columns
        for i in tqdm(range(i_max)):
            io_object: io.StringIO = io.StringIO()
            for j in range(interval):
                io_object.write(f.readline())
            df: pd.read_csv = pd.read_csv(io.StringIO(io_object.getvalue()), header=None, names=header,
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
            io_object.close()
            if len(df) == interval or ((i == i_max - 1) and len(df) == (rows - 1)%interval):
                raise Exception("Data parse error")
            for i_ticker, group_df in df.groupby("銘柄コード"):
                file_name: str = "save/{}/{}.csv".format(day, i_ticker)
                exists: bool = os.path.exists(file_name)
                group_df.to_csv(open(file_name, mode="a"), index=False, header=(not exists))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('day', help='processing day')
    args = parser.parse_args()
    split(day=args.day)
