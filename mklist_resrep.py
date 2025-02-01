import os
from pathlib import Path
import re
import shutil
import warnings

import pandas as pd
import requests
from urllib.parse import urlparse


### <Config>
CACHE_DIR = "cache"  # Files in this directory may be overwritten.
INDEX_FILE = "resrep.csv"  # will be overwritten
### </Config>


# User-Agent HTTP header value used when downloading.
# This is needed because connections fail without it. (wget and curl also fail without it.)
user_agent = "Mozilla/5.0"
# user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"  # long version


def _download(url: str, filepath: Path) -> Path:
    assert str is not None
    assert filepath is not None
    print(f"Downloading from {url} to {filepath}...")
    with requests.get(url, stream=True, headers={"User-Agent": user_agent}) as r:
        with open(filepath, "wb") as f:
            shutil.copyfileobj(r.raw, f)
    return filepath


def _get_filepath_safely(url: str, directory: str or Path = ".") -> Path:
    """ Convert to a path string, which is composed of `directory` and the basename part of `url`;
        e.g., https://example.com/a/b/foo.xlsx => rawfiles/foo.xlsx.
        Additionally, the basename part is checked for security (to avoid directory traversal)
        by forcing it to consist alphanumerics, '.', '_', and '-' only; if not, ValueException is raised.
    """
    if isinstance(directory, str):
        to_dir = Path(directory)
    elif isinstance(directory, Path):
        to_dir = directory
    else:
        raise ValueError("`directory` must be str or pathlib.Path, but is " + type(directory))
    u = urlparse(url)
    fn = os.path.basename(u.path)
    if not re.match(r"^[a-zA-Z0-9._-]+$", fn):
        raise ValueError("the basename part of `url` must only contain alphanumerics or '._-': " + fn )
    fp = to_dir / fn
    return fp


def download_file(url: str, to_dir: str or Path = ".") -> Path:
    return _download(url, _get_filepath_safely(url, to_dir))


def local_or_download_file(url: str, to_dir: str or Path = "."):
    filepath = _get_filepath_safely(url, to_dir)
    if filepath.exists() and filepath.is_file():
        return filepath
    else:
        return _download(url, filepath)


def cleanse(df: pd.DataFrame) -> None:
    # Replace any "\n" with "::"
    for i in range(len(df)):
        for j in range(len(df.columns)):
            s0 = str(df.iat[i, j])
            s = s0.strip()
            s = s.replace("\n", "::")
            if s != s0:
                df.iat[i, j] = s
    # Convert Gengo-format (supporting Reiwa only) dates to western format in `released_on` column
    dates = df["released_on"].to_list()
    for i, d in enumerate(dates):
        m = re.match(r"(\d+)\.(\d+)\.(\d+)", d)
        if m:
            reiwa = int(m.group(1))
            month = int(m.group(2))
            dayom = int(m.group(3))  # day-of-month
            assert reiwa < 50
            year = reiwa + 2019
            d = f"{year:4}{month:02}{dayom:02}"
            dates[i] = d
    df["released_on"] = dates



# Suppress warnings from openpyxl. I encountered "cannot handle header/footer" warnings that can be safely ignored.
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

os.makedirs(CACHE_DIR, exist_ok=True)
df_all = pd.DataFrame()

for fy in (2019, 2020, 2021, 2022, 2023):
    url = f"https://www.meti.go.jp/meti_lib/report/{fy}FY/itakuichiran{fy}FY.xlsx"
    filepath = local_or_download_file(url, CACHE_DIR)
    print(f"Reading from {filepath}...")
    df = pd.read_excel(filepath, skiprows=1)

    ja_cols = ('管理番号', '掲載日', 'ファイル\nコード', '委　託　調　査　報　告　書　名',
               '委託事業者名', '担当課室名', 'PDF・ZIP\n容量\n（ＭＢ）',
               'ＨＰアドレス（報告書）', 'ＨＰアドレス（データ）')
    en_cols = ('id', 'released_on', 'file_code', 'title',
               'consignee', 'consignor', 'size_mb',
               'document_url', 'data_url')

    # change column names
    for i, ja in enumerate(ja_cols):
        if ja != df.columns[i]:
            raise Exception(f"unknown column name '{df.columns[i]}' in {filepath}")
        cols = df.columns.to_list()
        cols[i] = en_cols[i]
        df.columns = cols

    df["document_url"] = df["document_url"].fillna("")
    df["data_url"] = df["data_url"].fillna("")

    # treat "cont'd" rows
    i = 0
    while i < len(df):
        i += 1
        while i < len(df):
            irow = df.index[i]
            jrow = df.index[i-1]
            if df.at[irow, "id"] != df.at[jrow, "id"]:  # only a row that has the same id with the prev. row
                break 
            if df.at[irow, "title"] != "（続き）":  # only a row that has "cont'd" in title column
                break
            # Note that there are rows having duplicate id but its title is not "cont'd",
            # both in consequtive rows or distant rows (even have different consignee). Yikes!
            du = df.at[irow, "document_url"]
            if du:
                jdu = df.at[jrow, "document_url"]
                if jdu:
                    df.at[jrow, "document_url"] = jdu + "," + du
                else:
                    df.at[jrow, "document_url"] = du
            tu = df.at[irow, "data_url"]
            if tu:
                jtu = df.at[jrow, "data_url"]
                if jtu:
                    df.at[jrow, "data_url"] = jtu + "," + tu
                else:
                    df.at[jrow, "data_url"] = tu
            df = df.drop(df.index[i])

    # give prefix to id
    ser_id = df["id"]
    L = ser_id.to_list()
    for i, _id in enumerate(L):
        L[i] = f"FY{fy}_{_id:06}"
    # de-duplicate ids (for rows with the same id but different documents, consignee, etc.)
    LL = []
    for i, _id in enumerate(L):
        eid = _id
        seq = 0
        while eid in LL:
           seq += 1
           eid = _id + "_" + str(seq)
        L[i] = eid
        LL.append(eid)

    df["id"] = L

    # concatenate
    df_all = pd.concat([df_all, df], axis=0)

cleanse(df_all)

# simple check
ser_id = df_all["id"]
if ser_id.nunique() == len(ser_id):
    print("OK: all `id`s are unique.")
else:
    nu = ser_id.nunique()
    su = len(ser_id)
    print(f"Warning: Duplcate `id`s found: count={su-nu}")
    # extract duplicated IDs
    dups = ser_id[ser_id.duplicated()].to_list()
    print(f"Warning: Duplicate `id`s: {dups}")
    #for dup in dups:
    #    print(df_all[df_all["id"]==dup])

print(f"Success: {len(df_all)} entries written to '{INDEX_FILE}'")
df_all.to_csv(INDEX_FILE, index=False)
