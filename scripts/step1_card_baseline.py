"""
Step 1: 카드매출 베이스라인 구축
==============================
- 월별 ZIP → 외지인/내지인 매출 시계열 추출
- 관광 업종별, 읍면동별 집계
- 방송 전후 비교 기초 데이터 생성

실행: python step1_card_baseline.py
"""
import pandas as pd
import numpy as np
import glob
import os
import sys
from pathlib import Path
from tqdm import tqdm

from config import *
from utils import read_zip_csv, list_card_zips, extract_yyyymm

print("=" * 60)
print("Step 1: 카드매출 베이스라인 구축")
print("=" * 60)

# ============================================================
# 1. ZIP 파일 목록 확인
# ============================================================
zips = list_card_zips(CARD_DIR)
print(f"\n카드매출 ZIP: {len(zips)}개")
for z in zips[:5]:
    print(f"  {os.path.basename(z)} ({extract_yyyymm(z)})")
if len(zips) > 5:
    print(f"  ... 외 {len(zips)-5}개")

# ============================================================
# 2. 외지인 → 아산시 소비 데이터 추출 (핵심: 관광 소비 proxy)
# ============================================================
# 파일 패턴: AS_*_CCND_EXCL_RSDT_CRTR_CCND_CSPT_DONG_*.csv
#   → 아산시 거주자가 아닌(EXCL_RSDT) 사람이 아산시에서(FRCS) 소비한 내역
#   → DONG 단위 (읍면동별), 업종/성별/연령 세분화

print("\n--- 외지인 소비 데이터 추출 ---")

monthly_outsider = []
weekly_outsider = []

for zpath in tqdm(zips, desc="ZIP 처리"):
    ym = extract_yyyymm(zpath)
    if not ym:
        continue

    # 월별 외지인 소비 (읍면동 단위)
    df_mm = read_zip_csv(zpath, "EXCL_RSDT_CRTR_CCND_CSPT_DONG")
    if df_mm is not None and len(df_mm) > 0:
        # 아산시 내 가맹점만 필터
        df_mm = df_mm[df_mm['FRCS_SGG_NM'] == '아산시'].copy()
        df_mm['CRTR_YM'] = ym
        monthly_outsider.append(df_mm)

    # 주별 외지인 소비 (읍면동 단위) - 더 세밀한 전후 비교용
    df_wk = read_zip_csv(zpath, "WEEK_CCND_EXCL_RSDT_CRTR_CCND_CSPT_DONG")
    if df_wk is not None and len(df_wk) > 0:
        df_wk = df_wk[df_wk['FRCS_SGG_NM'] == '아산시'].copy()
        weekly_outsider.append(df_wk)

if monthly_outsider:
    df_outsider_mm = pd.concat(monthly_outsider, ignore_index=True)
    print(f"  월별 외지인 소비: {len(df_outsider_mm):,}행, 기간: {df_outsider_mm['CRTR_YM'].min()} ~ {df_outsider_mm['CRTR_YM'].max()}")
else:
    df_outsider_mm = pd.DataFrame()
    print("  월별 외지인 소비: 데이터 없음")

if weekly_outsider:
    df_outsider_wk = pd.concat(weekly_outsider, ignore_index=True)
    print(f"  주별 외지인 소비: {len(df_outsider_wk):,}행")
else:
    df_outsider_wk = pd.DataFrame()
    print("  주별 외지인 소비: 데이터 없음")

# ============================================================
# 3. 전체 소비 데이터 (내지인 포함) - 비교 기준선
# ============================================================
print("\n--- 전체 소비 데이터 (내지인+외지인) ---")

monthly_total = []
for zpath in tqdm(zips, desc="전체 소비"):
    ym = extract_yyyymm(zpath)
    if not ym:
        continue
    # DSST_SPRT_AMT_CUST_CSPT: 읍면동별 전체 매출 요약
    df = read_zip_csv(zpath, "DSST_SPRT_AMT_CUST_CSPT")
    if df is not None and len(df) > 0:
        df = df[df['SGG_NM'] == '아산시'].copy()
        monthly_total.append(df)

if monthly_total:
    df_total_mm = pd.concat(monthly_total, ignore_index=True)
    print(f"  전체 월별: {len(df_total_mm):,}행")
else:
    df_total_mm = pd.DataFrame()
    print("  전체 월별: 데이터 없음")

# ============================================================
# 4. 업종/지역별 집계 → 시계열 생성
# ============================================================
print("\n--- 시계열 집계 ---")

if len(df_outsider_mm) > 0:
    # 4-1. 읍면동별 외지인 총 매출 (월별)
    agg_dong = df_outsider_mm.groupby(['CRTR_YM', 'FRCS_DONG_NM']).agg(
        outsider_amt=('ALL_USE_AMT', 'sum'),
        outsider_cnt=('ALL_USE_NOCS', 'sum'),
        frcs_cnt=('FRCS_CNT', 'sum'),
    ).reset_index()
    agg_dong.to_csv(OUTPUT_DIR / "card_outsider_by_dong_monthly.csv", index=False, encoding='utf-8-sig')
    print(f"  저장: card_outsider_by_dong_monthly.csv ({len(agg_dong):,}행)")

    # 4-2. 업종별 외지인 매출 (월별)
    agg_biz = df_outsider_mm.groupby(['CRTR_YM', 'TOBIZ_CD', 'TOBIZ_NM']).agg(
        outsider_amt=('ALL_USE_AMT', 'sum'),
        outsider_cnt=('ALL_USE_NOCS', 'sum'),
    ).reset_index()
    agg_biz.to_csv(OUTPUT_DIR / "card_outsider_by_biz_monthly.csv", index=False, encoding='utf-8-sig')
    print(f"  저장: card_outsider_by_biz_monthly.csv ({len(agg_biz):,}행)")

    # 4-3. 관광 핵심업종 x 방송노출 읍면동 (분석의 핵심 테이블)
    broadcast_dongs = set()
    for b in BROADCASTS:
        broadcast_dongs.update(b['dong_names'])

    tourism_codes = set(CORE_TOURISM_BIZ + FOOD_BIZ)
    mask_tourism = df_outsider_mm['TOBIZ_CD'].isin(tourism_codes)
    mask_dong = df_outsider_mm['FRCS_DONG_NM'].isin(broadcast_dongs)

    # 처치군: 관광업종 + 방송노출 읍면동
    treat = df_outsider_mm[mask_tourism & mask_dong].groupby(['CRTR_YM', 'FRCS_DONG_NM', 'TOBIZ_NM']).agg(
        outsider_amt=('ALL_USE_AMT', 'sum'),
        outsider_cnt=('ALL_USE_NOCS', 'sum'),
    ).reset_index()
    treat['group'] = 'treatment'

    # 대조군: 관광업종 + 방송 미노출 읍면동
    ctrl = df_outsider_mm[mask_tourism & ~mask_dong].groupby(['CRTR_YM', 'FRCS_DONG_NM', 'TOBIZ_NM']).agg(
        outsider_amt=('ALL_USE_AMT', 'sum'),
        outsider_cnt=('ALL_USE_NOCS', 'sum'),
    ).reset_index()
    ctrl['group'] = 'control'

    df_did = pd.concat([treat, ctrl], ignore_index=True)
    df_did.to_csv(OUTPUT_DIR / "card_did_panel_monthly.csv", index=False, encoding='utf-8-sig')
    print(f"  저장: card_did_panel_monthly.csv ({len(df_did):,}행)")

    # 4-4. 고객 거주지(출발지)별 유입 분석 - 어디서 오나
    agg_origin = df_outsider_mm.groupby(['CRTR_YM', 'CUST_CTPV_NM', 'CUST_SGG_NM']).agg(
        outsider_amt=('ALL_USE_AMT', 'sum'),
        outsider_cnt=('ALL_USE_NOCS', 'sum'),
    ).reset_index()
    agg_origin.to_csv(OUTPUT_DIR / "card_outsider_origin_monthly.csv", index=False, encoding='utf-8-sig')
    print(f"  저장: card_outsider_origin_monthly.csv ({len(agg_origin):,}행)")

    # 4-5. 성별/연령별 외지인 소비 (방송 타겟 데모 비교용)
    age_cols_nocs = [c for c in df_outsider_mm.columns if 'USE_NOCS' in c and c.startswith(('ML_', 'FM_', 'TWT_', 'TRT_', 'FRT_', 'FFT_', 'SXT_', 'SVT_'))]
    age_cols_amt = [c for c in df_outsider_mm.columns if 'USE_AMT' in c and c.startswith(('ML_', 'FM_', 'TWT_', 'TRT_', 'FRT_', 'FFT_', 'SXT_', 'SVT_'))]

    if age_cols_amt:
        agg_demo = df_outsider_mm.groupby('CRTR_YM')[age_cols_amt].sum().reset_index()
        agg_demo.to_csv(OUTPUT_DIR / "card_outsider_demo_monthly.csv", index=False, encoding='utf-8-sig')
        print(f"  저장: card_outsider_demo_monthly.csv")

# ============================================================
# 5. 주별 데이터 (방영 전후 세밀 비교용)
# ============================================================
if len(df_outsider_wk) > 0:
    agg_wk = df_outsider_wk.groupby(['CRTR_WEEK', 'FRCS_DONG_NM']).agg(
        outsider_amt=('ALL_USE_AMT', 'sum'),
        outsider_cnt=('ALL_USE_NOCS', 'sum'),
    ).reset_index()
    agg_wk.to_csv(OUTPUT_DIR / "card_outsider_by_dong_weekly.csv", index=False, encoding='utf-8-sig')
    print(f"  저장: card_outsider_by_dong_weekly.csv ({len(agg_wk):,}행)")

# ============================================================
# 6. 일별 데이터 (CSTMR/MER 테이블에서 - 더 세밀)
# ============================================================
print("\n--- 일별 외지인 매출 추출 ---")
daily_outsider = []

for zpath in tqdm(zips, desc="일별 CSTMR"):
    ym = extract_yyyymm(zpath)
    if not ym:
        continue
    # ASAN_CSTMR_DATA: 일별, 읍면동, 업종, 성별, 연령
    df = read_zip_csv(zpath, "ASAN_CSTMR_DATA")
    if df is not None and len(df) > 0:
        # 외지인 필터: MEGA_CTY_NO != 44 OR CTY_RGN_NO != 44200
        # CSTMR 데이터는 "고객 거주지"가 아산시 내인지 확인
        # CTY_RGN_NM이 '아산시'가 아닌 경우 = 외지인이 아산에서 소비
        outsider = df[df['CTY_RGN_NM'] != '아산시'].copy()
        if len(outsider) > 0:
            outsider_agg = outsider.groupby(['SALE_DATE', 'ADMI_CTY_NM', 'TP_GRP_NM', 'TP_BUZ_NM']).agg(
                sale_amt=('SALE_AMT', 'sum'),
                sale_cnt=('SALE_CNT', 'sum'),
            ).reset_index()
            daily_outsider.append(outsider_agg)

if daily_outsider:
    df_daily = pd.concat(daily_outsider, ignore_index=True)
    df_daily['SALE_DATE'] = pd.to_datetime(df_daily['SALE_DATE'].astype(str))
    df_daily.to_csv(OUTPUT_DIR / "card_outsider_daily.csv", index=False, encoding='utf-8-sig')
    print(f"  저장: card_outsider_daily.csv ({len(df_daily):,}행)")
    print(f"  기간: {df_daily['SALE_DATE'].min()} ~ {df_daily['SALE_DATE'].max()}")

# ============================================================
# 7. 전체 아산시 매출 요약 (내지인+외지인)
# ============================================================
if len(df_total_mm) > 0:
    # CRTR_YM이 컬럼에 있을 수도, 없을 수도
    ym_col = 'CRTR_YM' if 'CRTR_YM' in df_total_mm.columns else 'CRTR_WEEK'
    if ym_col in df_total_mm.columns:
        agg_total = df_total_mm.groupby([ym_col, 'DONG_NM']).agg(
            total_amt=('ALL_USE_AMT', 'sum'),
            total_cnt=('ALL_USE_NOCS', 'sum'),
        ).reset_index()
        agg_total.to_csv(OUTPUT_DIR / "card_total_by_dong_monthly.csv", index=False, encoding='utf-8-sig')
        print(f"\n  저장: card_total_by_dong_monthly.csv ({len(agg_total):,}행)")

print("\n" + "=" * 60)
print("Step 1 완료! 결과 파일 위치:", OUTPUT_DIR)
print("=" * 60)
