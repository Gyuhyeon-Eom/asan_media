"""
Step 5: 경제적 파급효과 산출
==========================
- 순수 방문객 증가분 x 1인당 관광소비액 = 직접 경제효과
- 프로그램별 예산 대비 ROI
- 업종별(음식/숙박/소매) 파급 구조

실행: python step5_economic_impact.py
선행: step1~3 결과 필요
"""
import pandas as pd
import numpy as np

from config import *

print("=" * 60)
print("Step 5: 경제적 파급효과 산출")
print("=" * 60)

# ============================================================
# 1. 데이터 로드
# ============================================================
try:
    df_card_dong = pd.read_csv(OUTPUT_DIR / "card_outsider_by_dong_monthly.csv", encoding='utf-8-sig')
    df_card_biz = pd.read_csv(OUTPUT_DIR / "card_outsider_by_biz_monthly.csv", encoding='utf-8-sig')
except FileNotFoundError:
    print("[!] Step 1 결과 파일 필요")
    df_card_dong = pd.DataFrame()
    df_card_biz = pd.DataFrame()

try:
    df_tmap_summary = pd.read_csv(OUTPUT_DIR / "tmap_broadcast_effect_summary.csv", encoding='utf-8-sig')
except FileNotFoundError:
    df_tmap_summary = pd.DataFrame()

try:
    df_did = pd.read_csv(OUTPUT_DIR / "did_results.csv", encoding='utf-8-sig')
except FileNotFoundError:
    df_did = pd.DataFrame()


# ============================================================
# 2. 1인당 관광소비액 추정 (카드매출 기반)
# ============================================================
print("\n--- 1인당 관광소비액 추정 ---")

if len(df_card_dong) > 0:
    # 외지인 건당 평균 결제액 (업종별)
    if len(df_card_biz) > 0:
        avg_per_tx = df_card_biz.groupby('TOBIZ_NM').agg(
            total_amt=('outsider_amt', 'sum'),
            total_cnt=('outsider_cnt', 'sum'),
        ).reset_index()
        avg_per_tx['avg_per_tx'] = (avg_per_tx['total_amt'] / avg_per_tx['total_cnt']).round(0)
        avg_per_tx = avg_per_tx.sort_values('total_amt', ascending=False)
        avg_per_tx.to_csv(OUTPUT_DIR / "avg_spending_per_transaction.csv", index=False, encoding='utf-8-sig')
        print(f"  업종별 건당 평균 결제액:")
        print(avg_per_tx.head(15).to_string(index=False))

    # 아산시 전체 외지인 월평균 소비 (1인당 추정)
    monthly_total = df_card_dong.groupby('CRTR_YM')['outsider_amt'].sum()
    monthly_cnt = df_card_dong.groupby('CRTR_YM')['outsider_cnt'].sum()
    avg_per_person = (monthly_total / monthly_cnt).mean()
    print(f"\n  외지인 1건당 평균 소비: {avg_per_person:,.0f}원")

    # 관광객 1인당 일 소비액 (복수 결제 감안, 보정계수 3~5건/인)
    TX_PER_PERSON = 3.5  # 1인당 평균 결제 건수 (한국관광공사 기준)
    per_capita_daily = avg_per_person * TX_PER_PERSON
    print(f"  관광객 1인당 일 소비 추정: {per_capita_daily:,.0f}원 (= {avg_per_person:,.0f} x {TX_PER_PERSON}건)")
else:
    per_capita_daily = 80000  # fallback: 한국관광공사 평균


# ============================================================
# 3. 프로그램별 경제적 파급효과
# ============================================================
print("\n--- 프로그램별 경제효과 ---")

econ_results = []
for b in BROADCASTS:
    name = b['name']
    budget = b['budget_1000won'] * 1000  # 원 단위

    # DID 결과에서 순수 효과 가져오기
    did_effect = np.nan
    if len(df_did) > 0:
        row = df_did[df_did['broadcast'] == name]
        if len(row) > 0:
            did_effect = row.iloc[0]['did_effect']

    # T맵 전후 비교 결과
    tmap_change_pct = np.nan
    post_daily = np.nan
    pre_daily = np.nan
    if len(df_tmap_summary) > 0:
        row = df_tmap_summary[df_tmap_summary['broadcast'] == name]
        if len(row) > 0:
            tmap_change_pct = row.iloc[0]['change_pct']
            post_daily = row.iloc[0]['post_daily_avg']
            pre_daily = row.iloc[0]['pre_daily_avg']

    # 순수 방문객 증가분 (일 단위)
    if not np.isnan(post_daily) and not np.isnan(pre_daily) and pre_daily > 0:
        daily_increase = post_daily - pre_daily
        days_effect = WINDOW_POST  # 효과 지속 기간
        total_increase = max(daily_increase * days_effect, 0)
    else:
        daily_increase = np.nan
        total_increase = np.nan

    # 경제적 효과 = 순수 증가 방문객 x 1인당 소비
    direct_effect = total_increase * per_capita_daily if not np.isnan(total_increase) else np.nan

    # 간접효과 (산업연관분석 승수: 관광업 평균 1.8배)
    MULTIPLIER = 1.8
    total_effect = direct_effect * MULTIPLIER if not np.isnan(direct_effect) else np.nan

    # ROI
    roi = (total_effect - budget) / budget * 100 if not np.isnan(total_effect) and budget > 0 else np.nan

    econ_results.append({
        'broadcast': name,
        'air_date': b['air_date'],
        'genre': b['genre'],
        'rating': b['rating'],
        'budget_원': budget,
        'daily_visitor_increase': round(daily_increase, 1) if not np.isnan(daily_increase) else np.nan,
        'total_visitor_increase_28d': round(total_increase, 0) if not np.isnan(total_increase) else np.nan,
        'per_capita_spend': per_capita_daily,
        'direct_effect_원': round(direct_effect, 0) if not np.isnan(direct_effect) else np.nan,
        'total_effect_원': round(total_effect, 0) if not np.isnan(total_effect) else np.nan,
        'roi_pct': round(roi, 1) if not np.isnan(roi) else np.nan,
        'did_effect': did_effect,
        'tmap_change_pct': round(tmap_change_pct, 1) if not np.isnan(tmap_change_pct) else np.nan,
    })

df_econ = pd.DataFrame(econ_results)
df_econ.to_csv(OUTPUT_DIR / "economic_impact_by_broadcast.csv", index=False, encoding='utf-8-sig')
print(f"\n  저장: economic_impact_by_broadcast.csv")
print(df_econ.to_string(index=False))


# ============================================================
# 4. 업종별 파급 구조 분석
# ============================================================
print("\n--- 업종별 파급 구조 ---")

if len(df_card_biz) > 0:
    # 방송 전후 업종별 매출 변화 (2025년 하반기 vs 상반기)
    df_card_biz['year'] = df_card_biz['CRTR_YM'].astype(str).str[:4].astype(int)
    df_card_biz['half'] = np.where(df_card_biz['CRTR_YM'].astype(str).str[4:6].astype(int) <= 6, 'H1', 'H2')

    # 2025년만 (방송 집중 시기)
    df_2025 = df_card_biz[df_card_biz['year'] == 2025]
    if len(df_2025) > 0:
        half_comp = df_2025.groupby(['TOBIZ_NM', 'half'])['outsider_amt'].sum().unstack(fill_value=0)
        if 'H1' in half_comp.columns and 'H2' in half_comp.columns:
            half_comp['change_pct'] = ((half_comp['H2'] - half_comp['H1']) / half_comp['H1'] * 100).round(1)
            half_comp = half_comp.sort_values('H2', ascending=False).head(20)
            half_comp.to_csv(OUTPUT_DIR / "biz_half_comparison_2025.csv", encoding='utf-8-sig')
            print(f"  저장: biz_half_comparison_2025.csv")
            print(f"\n  2025년 상/하반기 업종별 외지인 매출 비교 (상위 20):")
            print(half_comp.to_string())


# ============================================================
# 5. 종합 요약 테이블
# ============================================================
print("\n" + "=" * 60)
print("종합 요약")
print("=" * 60)

print(f"""
분석 기간: 카드매출 2019~ / T맵 2019~ / SKT 유동인구 2026~
분석 대상: 방송 {len(BROADCASTS)}건
1인당 추정 관광소비: {per_capita_daily:,.0f}원/일
산업연관 승수: {MULTIPLIER}배

※ 위 수치는 추정치이며, 다음 보완 필요:
  - SKT 유동인구 2025년 데이터 추가 시 방문객 수 직접 측정 가능
  - 아산페이 데이터 추가 시 지역 내 소비 효과 정밀 측정 가능
  - 네이버/구글 검색 트렌드 연동 시 온라인 관심도 교차검증 가능
""")

print("=" * 60)
print("Step 5 완료!")
print("=" * 60)
