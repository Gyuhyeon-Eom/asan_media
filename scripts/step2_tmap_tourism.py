"""
Step 2: T맵 내비게이션 - 관광지별 방문 시계열
==========================================
- 방송 노출 관광지별 일별 방문 추이
- 출발지 분석 (어디서 왔는지)
- 체류시간/다음 목적지 패턴

실행: python step2_tmap_tourism.py
"""
import pandas as pd
import numpy as np
import glob
import os
from tqdm import tqdm

from config import *
from utils import list_tmap_csvs

print("=" * 60)
print("Step 2: T맵 내비게이션 관광 분석")
print("=" * 60)

# ============================================================
# 1. T맵 파일 로드
# ============================================================
tmap_files = list_tmap_csvs(TMAP_DIR)
print(f"\nT맵 파일: {len(tmap_files)}개")

# 방송에서 노출된 주요 관광지 키워드
BROADCAST_POI_KEYWORDS = {
    "신정호": ["신정호", "신정호정원", "신정호공원"],
    "현충사": ["현충사"],
    "외암민속마을": ["외암", "외암민속"],
    "곡교천": ["곡교천", "은행나무길"],
    "온양온천": ["온양온천", "온천", "족욕", "온천시장"],
    "도고온천": ["도고", "도고파라다이스", "도고온천"],
    "피나클랜드": ["피나클랜드"],
    "영인산": ["영인산"],
    "아산스파포레": ["스파포레"],
    "세계꽃식물원": ["꽃식물원", "세계꽃"],
    "이순신관광체험센터": ["이순신", "이순신관광"],
}

def match_poi(dstn_nm, keywords_dict):
    """목적지명을 관광지 키워드로 매칭"""
    if pd.isna(dstn_nm):
        return None
    dstn_lower = str(dstn_nm).lower()
    for poi, keywords in keywords_dict.items():
        for kw in keywords:
            if kw in dstn_lower:
                return poi
    return None


# ============================================================
# 2. 전체 T맵 데이터 로드 + 관광지 매칭
# ============================================================
print("\n--- T맵 데이터 로드 ---")

chunks = []
for f in tqdm(tmap_files, desc="T맵 로드"):
    try:
        df = pd.read_csv(f, encoding='utf-8-sig')
    except:
        try:
            df = pd.read_csv(f, encoding='cp949')
        except:
            print(f"  읽기 실패: {os.path.basename(f)}")
            continue
    chunks.append(df)

if not chunks:
    print("T맵 데이터 없음. 종료.")
    exit()

df_tmap = pd.concat(chunks, ignore_index=True)
df_tmap['drv_ymd'] = pd.to_datetime(df_tmap['drv_ymd'].astype(str))
print(f"  전체: {len(df_tmap):,}행, {df_tmap['drv_ymd'].min()} ~ {df_tmap['drv_ymd'].max()}")

# 관광지 매칭
df_tmap['matched_poi'] = df_tmap['dstn_nm'].apply(lambda x: match_poi(x, BROADCAST_POI_KEYWORDS))
df_tmap['is_tourism'] = df_tmap['dstn_ctgy'].str.startswith('여행/레저').fillna(False)
print(f"  관광지 매칭: {df_tmap['matched_poi'].notna().sum():,}행")
print(f"  관광카테고리: {df_tmap['is_tourism'].sum():,}행")

# ============================================================
# 3. 관광지별 일별 방문 시계열
# ============================================================
print("\n--- 관광지별 일별 시계열 ---")

# 3-1. 매칭된 주요 관광지
poi_daily = df_tmap[df_tmap['matched_poi'].notna()].groupby(
    ['drv_ymd', 'matched_poi']
).agg(
    visit_cnt=('vst_cnt', 'sum'),
    avg_stay_min=('avg_stay_min', 'mean'),
    avg_drv_dstc=('avg_drv_dstc', 'mean'),
    unique_origins=('frst_dptre_sgg_nm', 'nunique'),
).reset_index()

poi_daily.to_csv(OUTPUT_DIR / "tmap_poi_daily.csv", index=False, encoding='utf-8-sig')
print(f"  저장: tmap_poi_daily.csv ({len(poi_daily):,}행)")

# 3-2. 전체 관광 카테고리 (넓은 범위)
tourism_daily = df_tmap[df_tmap['is_tourism']].groupby('drv_ymd').agg(
    total_visits=('vst_cnt', 'sum'),
    total_pois=('dstn_nm', 'nunique'),
    avg_stay=('avg_stay_min', 'mean'),
).reset_index()

tourism_daily.to_csv(OUTPUT_DIR / "tmap_tourism_daily.csv", index=False, encoding='utf-8-sig')
print(f"  저장: tmap_tourism_daily.csv ({len(tourism_daily):,}행)")

# ============================================================
# 4. 출발지 분석 (어디서 왔는지)
# ============================================================
print("\n--- 출발지 분석 ---")

# 방송 노출 관광지의 출발지별 방문
poi_origin = df_tmap[df_tmap['matched_poi'].notna()].groupby(
    ['drv_ymd', 'matched_poi', 'frst_dptre_ctpv_nm', 'frst_dptre_sgg_nm']
).agg(
    visit_cnt=('vst_cnt', 'sum'),
).reset_index()

poi_origin.to_csv(OUTPUT_DIR / "tmap_poi_origin_daily.csv", index=False, encoding='utf-8-sig')
print(f"  저장: tmap_poi_origin_daily.csv ({len(poi_origin):,}행)")

# 월별 출발지 요약 (Top 출발지 파악)
poi_origin['ym'] = poi_origin['drv_ymd'].dt.to_period('M').astype(str)
origin_monthly = poi_origin.groupby(
    ['ym', 'matched_poi', 'frst_dptre_ctpv_nm']
).agg(visit_cnt=('visit_cnt', 'sum')).reset_index()
origin_monthly = origin_monthly.sort_values(['ym', 'matched_poi', 'visit_cnt'], ascending=[True, True, False])
origin_monthly.to_csv(OUTPUT_DIR / "tmap_poi_origin_monthly.csv", index=False, encoding='utf-8-sig')
print(f"  저장: tmap_poi_origin_monthly.csv")

# ============================================================
# 5. 성별/연령별 방문 패턴 (타겟 데모 매칭)
# ============================================================
print("\n--- 성별/연령별 패턴 ---")

demo_cols = ['fm_user_cnt', 'ml_user_cnt', 'twt_les_user_cnt',
             'trt_user_cnt', 'frt_user_cnt', 'fft_user_cnt', 'sxt_abv_user_cnt']
existing_demo = [c for c in demo_cols if c in df_tmap.columns]

if existing_demo:
    poi_demo = df_tmap[df_tmap['matched_poi'].notna()].groupby(
        ['drv_ymd', 'matched_poi']
    )[existing_demo].sum().reset_index()
    poi_demo.to_csv(OUTPUT_DIR / "tmap_poi_demo_daily.csv", index=False, encoding='utf-8-sig')
    print(f"  저장: tmap_poi_demo_daily.csv")

# ============================================================
# 6. 방송 전후 비교 요약 테이블
# ============================================================
print("\n--- 방송 전후 비교 ---")

results = []
for b in BROADCASTS:
    air = pd.Timestamp(b['air_date'])
    air_end = pd.Timestamp(b['air_end_date'])

    # 해당 방송의 노출 관광지
    poi_keywords_for_broadcast = []
    for loc in b['locations']:
        for poi, kws in BROADCAST_POI_KEYWORDS.items():
            if any(kw in loc for kw in kws) or poi in loc:
                poi_keywords_for_broadcast.append(poi)

    if not poi_keywords_for_broadcast:
        # 특정 관광지 없으면 전체 관광 카테고리로
        mask_poi = df_tmap['is_tourism']
        poi_label = "전체관광"
    else:
        mask_poi = df_tmap['matched_poi'].isin(poi_keywords_for_broadcast)
        poi_label = "+".join(poi_keywords_for_broadcast[:3])

    # 전 4주
    pre_start = air - pd.Timedelta(days=WINDOW_PRE)
    mask_pre = (df_tmap['drv_ymd'] >= pre_start) & (df_tmap['drv_ymd'] < air)
    pre_visits = df_tmap[mask_poi & mask_pre]['vst_cnt'].sum()
    pre_days = (air - pre_start).days

    # 후 4주
    post_end = air_end + pd.Timedelta(days=WINDOW_POST)
    mask_post = (df_tmap['drv_ymd'] > air_end) & (df_tmap['drv_ymd'] <= post_end)
    post_visits = df_tmap[mask_poi & mask_post]['vst_cnt'].sum()
    post_days = (post_end - air_end).days

    # 전년 동기 (있으면)
    yoy_air = air - pd.Timedelta(days=365)
    yoy_post = yoy_air + pd.Timedelta(days=WINDOW_POST)
    mask_yoy = (df_tmap['drv_ymd'] >= yoy_air) & (df_tmap['drv_ymd'] <= yoy_post)
    yoy_visits = df_tmap[mask_poi & mask_yoy]['vst_cnt'].sum()

    results.append({
        'broadcast': b['name'],
        'air_date': b['air_date'],
        'poi': poi_label,
        'pre_daily_avg': pre_visits / max(pre_days, 1),
        'post_daily_avg': post_visits / max(post_days, 1),
        'change_pct': ((post_visits/max(post_days,1)) - (pre_visits/max(pre_days,1))) / max(pre_visits/max(pre_days,1), 1) * 100 if pre_visits > 0 else np.nan,
        'yoy_daily_avg': yoy_visits / max(WINDOW_POST, 1),
        'yoy_change_pct': ((post_visits/max(post_days,1)) - (yoy_visits/max(WINDOW_POST,1))) / max(yoy_visits/max(WINDOW_POST,1), 1) * 100 if yoy_visits > 0 else np.nan,
    })

df_results = pd.DataFrame(results)
df_results.to_csv(OUTPUT_DIR / "tmap_broadcast_effect_summary.csv", index=False, encoding='utf-8-sig')
print(f"  저장: tmap_broadcast_effect_summary.csv")
print(df_results.to_string(index=False))

print("\n" + "=" * 60)
print("Step 2 완료!")
print("=" * 60)
