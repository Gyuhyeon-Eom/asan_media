# # 아산시 데이터 탐색 (Data Check)
# > 각 데이터 소스별 head, shape, 컬럼 타입, 기본 통계 확인
# 

import pandas as pd
import numpy as np
import zipfile, os, glob
from pathlib import Path

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 40)
pd.set_option('display.width', 200)

# 데이터 경로 (환경에 맞게 수정)
# 데이터 경로
BASE_DIR = r"C:\Users\HP\IdeaProjects\sundo\asan"
DATA_DIRS = {
    "관광데이터랩": r"C:\Users\HP\Desktop\01.데이터",
    "카드매출": os.path.join(BASE_DIR, "02. 카드매출 데이터", "02. 카드매출 데이터"),
    "신용정보": os.path.join(BASE_DIR, "03. 신용정보 데이터", "03. 신용정보 데이터"),
    "내비게이션": os.path.join(BASE_DIR, "04. 내비게이션 데이터", "04. 내비게이션 데이터"),
    "인구": os.path.join(BASE_DIR, "01. 인구 데이터", "01. 인구 데이터"),
}
DATA_DIR = DATA_DIRS["관광데이터랩"]
print("데이터 경로:")
for name, path in DATA_DIRS.items():
    exists = os.path.exists(path)
    print(f"  {name}: {path} {'[OK]' if exists else '[NOT FOUND]'}")


def read_zip_csv(zip_path, csv_name, nrows=5):
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(csv_name) as f:
            for enc in ['utf-8-sig','utf-8','cp949','euc-kr']:
                try:
                    f.seek(0)
                    return pd.read_csv(f, nrows=nrows, encoding=enc)
                except: continue
    return None


# ---
# ## 1. 카드매출 데이터
# 

card_zips = sorted(glob.glob(os.path.join(DATA_DIRS["카드매출"], "**/*.zip"), recursive=True))
if not card_zips:
    card_zips = sorted(glob.glob(os.path.join(DATA_DIRS["카드매출"], "*.zip")))
# ZIP 없으면 CSV 직접 탐색
card_csvs = sorted(glob.glob(os.path.join(DATA_DIRS["카드매출"], "**/*.csv"), recursive=True))
print(f"카드매출 CSV (ZIP 외): {len(card_csvs)}개")
for f in card_csvs[:5]:
    print(f"  {os.path.basename(f)}")
print(f"카드매출 ZIP: {len(card_zips)}개")
if card_zips:
    with zipfile.ZipFile(card_zips[0]) as zf:
        for n in sorted(zf.namelist()):
            print(f"  {n} ({zf.getinfo(n).file_size/1024:.0f}KB)")


# 각 CSV head 확인
if card_zips:
    with zipfile.ZipFile(card_zips[0]) as zf:
        for csv_name in sorted(n for n in zf.namelist() if n.endswith('.csv')):
            df = read_zip_csv(card_zips[0], csv_name, nrows=3)
            if df is not None:
                print(f"\n{'='*80}")
                print(f"{csv_name} | 컬럼: {len(df.columns)}개")
                print(f"컬럼명: {list(df.columns)}")
                print(df.head(3).to_string())


# ### ASAN_CSTMR_DATA 상세
# 

if card_zips:
    with zipfile.ZipFile(card_zips[0]) as zf:
        f = [n for n in zf.namelist() if 'CSTMR' in n]
        if f:
            df = read_zip_csv(card_zips[0], f[0], nrows=5000)
            print(f"{f[0]} | Shape: {df.shape}")
            print(f"\n타입:\n{df.dtypes}")
            print(f"\nSEX: {df['SEX'].value_counts().to_dict()}")
            print(f"AGE: {sorted(df['AGE'].unique())}")
            print(f"\n통계:\n{df.describe()}")
            print(df.head().to_string())


# ---
# ## 2. KCB 신용정보
# 

kcb_zips = sorted(glob.glob(os.path.join(DATA_DIRS["신용정보"], "**/*.zip"), recursive=True))
if not kcb_zips:
    kcb_zips = sorted(glob.glob(os.path.join(DATA_DIRS["신용정보"], "*.zip")))
print(f"KCB ZIP: {len(kcb_zips)}개")
if kcb_zips:
    with zipfile.ZipFile(kcb_zips[0]) as zf:
        for n in sorted(zf.namelist()):
            print(f"  {n} ({zf.getinfo(n).file_size/1024:.0f}KB) {'** PIA 암호화 **' if '.pia' in n else ''}")


if kcb_zips:
    with zipfile.ZipFile(kcb_zips[0]) as zf:
        for csv_name in sorted(n for n in zf.namelist() if n.endswith('.csv')):
            df = read_zip_csv(kcb_zips[0], csv_name, nrows=5)
            if df is not None:
                print(f"\n{'='*80}")
                print(f"{csv_name} | 컬럼: {len(df.columns)}개")
                print(df.head(3).to_string())


# ---
# ## 3. T맵 내비게이션
# 

tmap_files = sorted(glob.glob(os.path.join(DATA_DIRS["내비게이션"], "**/*.csv"), recursive=True))
if not tmap_files:
    tmap_files = sorted(glob.glob(os.path.join(DATA_DIRS["내비게이션"], "*.csv")))
print(f"T맵 파일: {len(tmap_files)}개")
if tmap_files:
    df = pd.read_csv(tmap_files[0], nrows=1000, encoding='utf-8-sig')
    print(f"\n{os.path.basename(tmap_files[0])} | Shape: {df.shape}")
    print(f"\n타입:\n{df.dtypes}")
    print(f"\n목적지 카테고리 TOP 10:\n{df['dstn_ctgy'].value_counts().head(10)}")
    print(df.head().to_string())


# ---
# ## 4. SKT 유동인구 (대용량)
# 

# 인구 데이터는 202601, 202602 폴더에 Excel 파일로 존재
pop_base = DATA_DIRS["인구"]
skt_files = []
for sub in ["202601", "202602"]:
    folder = os.path.join(pop_base, sub)
    if os.path.exists(folder):
        skt_files += sorted(glob.glob(os.path.join(folder, "AS_SKT_*")))
        skt_files += sorted(glob.glob(os.path.join(folder, "AS_HYUNDAE_*")))
print(f"SKT 파일: {len(skt_files)}개")
for f in skt_files:
    sz = os.path.getsize(f)/(1024**3)
    print(f"  {os.path.basename(f)}: {sz:.1f}GB" if sz>0.1 else f"  {os.path.basename(f)}: {os.path.getsize(f)/1024/1024:.0f}MB")


# SKT head만 (대용량 주의)
for f in skt_files[:6]:
    fname = os.path.basename(f)
    try:
        sz = os.path.getsize(f)/1024/1024
    except:
        sz = 0
    
    # 500MB 이상은 head만 (Excel은 전체 로드되므로 주의)
    if sz > 500:
        print(f"\n{'='*80}")
        print(f"{fname} | {sz:.0f}MB - 대용량! openpyxl read_only로 head만 읽기")
        try:
            import openpyxl
            wb = openpyxl.load_workbook(f, read_only=True)
            ws = wb.active
            rows = []
            for i, row in enumerate(ws.iter_rows(values_only=True)):
                rows.append(row)
                if i >= 5: break
            wb.close()
            if rows:
                df = pd.DataFrame(rows[1:], columns=rows[0])
                print(f"컬럼: {list(df.columns)}")
                print(df.head().to_string())
        except Exception as e:
            print(f"읽기 실패: {e}")
        continue
    
    try:
        if fname.endswith('.xlsx') or fname.endswith('.xls'):
            df = pd.read_excel(f, nrows=5)
        else:
            df = pd.read_csv(f, nrows=5, encoding='utf-8-sig')
    except Exception as e:
        print(f"읽기 실패: {fname} - {e}")
        continue
    
    print(f"\n{'='*80}")
    print(f"{fname} | {sz:.0f}MB | 컬럼: {len(df.columns)}개")
    print(f"컬럼: {list(df.columns)}")
    print(df.head().to_string())


# ---
# ## 5. 인구 데이터
# 

# 인구 데이터 전체 파일 목록 (202601, 202602)
pop_base = DATA_DIRS["인구"]
pop_files = []
for sub in ["202601", "202602"]:
    folder = os.path.join(pop_base, sub)
    if os.path.exists(folder):
        for f in sorted(os.listdir(folder)):
            fp = os.path.join(folder, f)
            if os.path.isfile(fp):
                pop_files.append(fp)

print(f"인구/SKT 전체 파일: {len(pop_files)}개")
print(f"{'파일명':<55} {'용량':>10}")
print("-"*67)
for f in pop_files:
    try:
        sz = os.path.getsize(f)/1024/1024
        unit = "MB" if sz < 1024 else "GB"
        val = sz if sz < 1024 else sz/1024
        print(f"  {os.path.basename(f):<53} {val:>7.1f}{unit}")
    except:
        print(f"  {os.path.basename(f):<53} (접근불가)")


# ---
# ## 6. 전체 파일 요약
# 

all_files = []
for root, dirs, files in os.walk(BASE_DIR):
    for f in files:
        fp = os.path.join(root, f)
        all_files.append({
            '파일': os.path.relpath(fp, DATA_DIR),
            '용량(MB)': round(os.path.getsize(fp)/1024/1024, 1) if os.path.isfile(fp) else 0,
            '확장자': os.path.splitext(f)[1]
        })

df_all = pd.DataFrame(all_files).sort_values('용량(MB)', ascending=False)
print(f"전체 파일: {len(df_all)}개 | 총 용량: {df_all['용량(MB)'].sum()/1024:.1f}GB")
print(f"\n확장자별:")
print(df_all.groupby('확장자')['용량(MB)'].agg(['count','sum']).sort_values('sum', ascending=False))
print(f"\n상위 20개:")
print(df_all.head(20).to_string())

