"""
공통 유틸리티 함수
"""
import pandas as pd
import numpy as np
import zipfile
import glob
import os
from pathlib import Path


def read_zip_csv(zip_path, csv_name=None, nrows=None, usecols=None):
    """ZIP 내부 CSV 읽기 (인코딩 자동 감지)"""
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n.endswith('.csv')]
        if csv_name:
            names = [n for n in names if csv_name in n]
        if not names:
            return None
        target = names[0]
        for enc in ['utf-8-sig', 'utf-8', 'cp949', 'euc-kr']:
            try:
                with zf.open(target) as f:
                    return pd.read_csv(f, encoding=enc, nrows=nrows, usecols=usecols)
            except (UnicodeDecodeError, UnicodeError):
                continue
    return None


def list_card_zips(card_dir):
    """카드매출 ZIP 파일 목록 (YYYYMM 순 정렬)"""
    zips = sorted(glob.glob(str(Path(card_dir) / "**" / "*.zip"), recursive=True))
    return zips


def list_tmap_csvs(tmap_dir):
    """T맵 CSV 파일 목록"""
    return sorted(glob.glob(str(Path(tmap_dir) / "**" / "as_tmap_od_*.csv"), recursive=True))


def list_skt_files(pop_dir, pattern="AS_SKT_*"):
    """SKT 유동인구 파일 목록"""
    return sorted(glob.glob(str(Path(pop_dir) / "**" / pattern), recursive=True))


def extract_yyyymm(filepath):
    """파일명에서 YYYYMM 추출"""
    import re
    base = os.path.basename(filepath)
    m = re.search(r'(\d{6})', base)
    return m.group(1) if m else None


def add_broadcast_windows(df, date_col, broadcasts):
    """방송 이벤트 윈도우 더미 변수 추가"""
    from config import WINDOW_PRE, WINDOW_POST
    df[date_col] = pd.to_datetime(df[date_col].astype(str))
    for b in broadcasts:
        air = pd.Timestamp(b['air_date'])
        col = b['name'].replace(' ', '_')
        # 전/중/후
        df[f'{col}_pre'] = ((df[date_col] >= air - pd.Timedelta(days=WINDOW_PRE)) &
                            (df[date_col] < air)).astype(int)
        df[f'{col}_post'] = ((df[date_col] > air) &
                             (df[date_col] <= air + pd.Timedelta(days=WINDOW_POST))).astype(int)
        df[f'{col}_air'] = (df[date_col] == air).astype(int)
        # air_end가 다른 경우 (같이삽시다 등 연속방영)
        if b['air_end_date'] != b['air_date']:
            air_end = pd.Timestamp(b['air_end_date'])
            df[f'{col}_during'] = ((df[date_col] >= air) &
                                   (df[date_col] <= air_end)).astype(int)
    return df


def get_holiday_calendar(start='2019-01-01', end='2026-12-31'):
    """한국 공휴일 달력 (수동 정의 - 주요 연휴)"""
    holidays = []
    for year in range(2019, 2027):
        holidays.extend([
            f"{year}-01-01",  # 신정
            f"{year}-03-01",  # 삼일절
            f"{year}-05-05",  # 어린이날
            f"{year}-06-06",  # 현충일
            f"{year}-08-15",  # 광복절
            f"{year}-10-03",  # 개천절
            f"{year}-10-09",  # 한글날
            f"{year}-12-25",  # 크리스마스
        ])
    # 설/추석 (변동 - 주요 연도만)
    lunar_holidays = [
        # 2024 설: 2/9~12, 추석: 9/16~18
        "2024-02-09", "2024-02-10", "2024-02-11", "2024-02-12",
        "2024-09-16", "2024-09-17", "2024-09-18",
        # 2025 설: 1/28~30, 추석: 10/5~7
        "2025-01-28", "2025-01-29", "2025-01-30",
        "2025-10-05", "2025-10-06", "2025-10-07",
        # 2026 설: 2/16~18, 추석: 9/24~26
        "2026-02-16", "2026-02-17", "2026-02-18",
        "2026-09-24", "2026-09-25", "2026-09-26",
    ]
    holidays.extend(lunar_holidays)
    return pd.to_datetime(holidays)


def yoy_comparison(df, date_col, value_col, group_cols=None):
    """전년 동기 대비 변화율 계산"""
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    df['year'] = df[date_col].dt.year
    df['month'] = df[date_col].dt.month
    df['day'] = df[date_col].dt.day

    if group_cols:
        idx = group_cols + ['month', 'day']
    else:
        idx = ['month', 'day']

    pivot = df.pivot_table(values=value_col, index=idx, columns='year', aggfunc='sum')
    for y in sorted(pivot.columns)[1:]:
        prev = y - 1
        if prev in pivot.columns:
            pivot[f'yoy_{y}'] = (pivot[y] - pivot[prev]) / pivot[prev] * 100
    return pivot.reset_index()


def safe_division(a, b, fill=0):
    """안전한 나눗셈"""
    return np.where(b != 0, a / b, fill)
