import numpy as np
import pandas as pd

INFILE = "resrep_FY2019-2023.csv"

def by_title_keyword(df: pd.DataFrame, s: str) -> pd.DataFrame:
    return df[df["title"].str.contains(s)]

def by_consignor(df: pd.DataFrame, s: str) -> pd.DataFrame:
    return df[df["consignor"] == s]

def by_consignor_dept(df: pd.DataFrame, s: str) -> pd.DataFrame:
    return df[df["consignor"].str.startswith(s)]

def by_consignee(df: pd.DataFrame, s: str) -> pd.DataFrame:
    return df[df["consignee"] == s]

def top_consignees(df: pd.DataFrame) -> pd.Series:
    return df["consignee"].value_counts()

def print_some_consignees_count(df2):
    for org in ("株式会社三菱総合研究所", "株式会社野村総合研究所", "みずほ情報総研株式会社", "株式会社エヌ・ティ・ティ・データ経営研究所"):
        df3 = by_consignee(df2, org)
        print(f"{len(df3)}: {dept} + {org}")


if __name__ == "__main__":
    df = pd.read_csv(INFILE)
    print(f"{len(df)}: total entries")
    print()

    dept = "資源エネルギー庁"
    df2 = by_consignor_dept(df, dept)
    print(f"{len(df2)}: consignee startswith {dept}")
    ser = top_consignees(df2)[:5]
    print(np.array(list(zip(ser.values, ser.index.array))))

    print()

    dept = "産業保安グループ"
    df2 = by_consignor_dept(df, dept)
    print(f"{len(df2)}: consignee startswith {dept}")
    ser = top_consignees(df2)[:5]
    print(np.array(list(zip(ser.values, ser.index.array))))

    print()

    consignor = "商務情報政策局::サイバーセキュリティ課"
    df2 = by_consignor(df, consignor)
    print(f"{len(df2)}: {consignor=}")
    ser = top_consignees(df2)[:5]
    print(np.array(list(zip(ser.values, ser.index.array))))

    print()
    keyword = "原子力"
    df2 = by_title_keyword(df, keyword)
    print(f"{len(df2)}: {keyword} in title")
    ser = top_consignees(df2)[:5]
    print(np.array(list(zip(ser.values, ser.index.array))))
    ser = df2["consignor"].value_counts()
    print(" consignors:")
    print(np.array(list(zip(ser.values, ser.index.array))))

    print()
    keyword = "放射性廃棄物"
    df2 = by_title_keyword(df, keyword)
    print(f"{len(df2)}: {keyword} in title")
    ser = top_consignees(df2)[:5]
    print(np.array(list(zip(ser.values, ser.index.array))))
    ser = df2["consignor"].value_counts()
    print(" consignors:")
    print(np.array(list(zip(ser.values, ser.index.array))))

