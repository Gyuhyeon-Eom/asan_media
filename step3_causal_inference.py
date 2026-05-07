"""
Step 3: 인과추론 - 순수 방송효과 추정
===================================
방법론:
  1. DID (이중차분법): 처치/대조 읍면동 비교
  2. CausalImpact: 합성대조군 시계열 인과추론
  3. 계절 분해(STL): 베이스라인에서 이상치 탐지

실행: python step3_causal_inference.py
선행: step1, step2 결과 파일 필요
"""
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

from config import *

print("=" * 60)
print("Step 3: 인과추론 - 순수 방송효과 추정")
print("=" * 60)

# ============================================================
# 1. 데이터 로드 (Step 1, 2 결과)
# ============================================================
print("\n--- 데이터 로드 ---")

try:
    df_card_dong = pd.read_csv(OUTPUT_DIR / "card_outsider_by_dong_monthly.csv", encoding='utf-8-sig')
    print(f"  카드 읍면동별: {len(df_card_dong):,}행")
except FileNotFoundError:
    print("  [!] card_outsider_by_dong_monthly.csv 없음 → Step 1 먼저 실행")
    df_card_dong = pd.DataFrame()

try:
    df_card_daily = pd.read_csv(OUTPUT_DIR / "card_outsider_daily.csv", encoding='utf-8-sig', parse_dates=['SALE_DATE'])
    print(f"  카드 일별: {len(df_card_daily):,}행")
except FileNotFoundError:
    print("  [!] card_outsider_daily.csv 없음")
    df_card_daily = pd.DataFrame()

try:
    df_tmap_poi = pd.read_csv(OUTPUT_DIR / "tmap_poi_daily.csv", encoding='utf-8-sig', parse_dates=['drv_ymd'])
    print(f"  T맵 관광지: {len(df_tmap_poi):,}행")
except FileNotFoundError:
    print("  [!] tmap_poi_daily.csv 없음 → Step 2 먼저 실행")
    df_tmap_poi = pd.DataFrame()

try:
    df_did_panel = pd.read_csv(OUTPUT_DIR / "card_did_panel_monthly.csv", encoding='utf-8-sig')
    print(f"  DID 패널: {len(df_did_panel):,}행")
except FileNotFoundError:
    df_did_panel = pd.DataFrame()


# ============================================================
# 2. STL 계절 분해 → 베이스라인 + 잔차(이상 탐지)
# ============================================================
print("\n--- [방법1] STL 계절 분해 ---")

def stl_decompose(series, period=12):
    """STL 분해 (statsmodels 필요)"""
    try:
        from statsmodels.tsa.seasonal import STL
        result = STL(series.dropna(), period=period, robust=True).fit()
        return result
    except ImportError:
        print("  [!] statsmodels 설치 필요: pip install statsmodels")
        return None
    except Exception as e:
        print(f"  [!] STL 실패: {e}")
        return None

if len(df_card_dong) > 0:
    # 아산시 전체 외지인 매출 월별 시계열
    ts = df_card_dong.groupby('CRTR_YM')['outsider_amt'].sum().reset_index()
    ts['CRTR_YM'] = pd.to_datetime(ts['CRTR_YM'], format='%Y%m')
    ts = ts.set_index('CRTR_YM').sort_index()
    ts = ts.asfreq('MS')  # 월초 frequency

    if len(ts) >= 24:  # 최소 2년치
        result = stl_decompose(ts['outsider_amt'], period=12)
        if result:
            decomp = pd.DataFrame({
                'date': ts.index,
                'observed': result.observed,
                'trend': result.trend,
                'seasonal': result.seasonal,
                'residual': result.resid,
            })
            decomp.to_csv(OUTPUT_DIR / "stl_outsider_total.csv", index=False, encoding='utf-8-sig')
            print(f"  저장: stl_outsider_total.csv")

            # 잔차에서 방송 시점 이상치 탐지
            resid_std = decomp['residual'].std()
            resid_mean = decomp['residual'].mean()
            decomp['is_anomaly'] = abs(decomp['residual'] - resid_mean) > 2 * resid_std
            anomalies = decomp[decomp['is_anomaly']]
            print(f"  이상치(2sigma): {len(anomalies)}개월")
            if len(anomalies) > 0:
                print(anomalies[['date', 'observed', 'trend', 'residual']].to_string(index=False))
    else:
        print(f"  데이터 부족 ({len(ts)}개월 < 24개월)")

    # 주요 관광 읍면동별 분해
    for dong in ["온양1동", "온양5동", "염치읍", "도고면"]:
        ts_dong = df_card_dong[df_card_dong['FRCS_DONG_NM'] == dong].copy()
        if len(ts_dong) < 24:
            continue
        ts_dong['CRTR_YM'] = pd.to_datetime(ts_dong['CRTR_YM'], format='%Y%m')
        ts_dong = ts_dong.set_index('CRTR_YM').sort_index()['outsider_amt'].asfreq('MS')
        result = stl_decompose(ts_dong, period=12)
        if result:
            decomp = pd.DataFrame({
                'date': ts_dong.index,
                'observed': result.observed,
                'trend': result.trend,
                'seasonal': result.seasonal,
                'residual': result.resid,
            })
            fname = f"stl_outsider_{dong}.csv"
            decomp.to_csv(OUTPUT_DIR / fname, index=False, encoding='utf-8-sig')
            print(f"  저장: {fname}")


# ============================================================
# 3. DID (이중차분법)
# ============================================================
print("\n--- [방법2] DID 이중차분법 ---")

def run_did(panel, time_col, value_col, group_col, treatment_time, broadcast_name):
    """
    Simple DID:
      Y = a + b*Post + c*Treat + d*(Post*Treat) + e
      d = 순수 방송효과 (DID estimator)
    """
    panel = panel.copy()
    panel['post'] = (panel[time_col] >= treatment_time).astype(int)
    panel['treat'] = (panel[group_col] == 'treatment').astype(int)
    panel['did'] = panel['post'] * panel['treat']

    # 그룹별 평균
    summary = panel.groupby(['post', 'treat'])[value_col].mean().unstack()
    print(f"\n  [{broadcast_name}]")
    print(f"  Treatment Time: {treatment_time}")
    print(f"  Group means:")
    print(summary.to_string())

    # DID 추정
    try:
        from statsmodels.formula.api import ols
        model = ols(f'{value_col} ~ post * treat', data=panel).fit()
        did_coef = model.params.get('post:treat', np.nan)
        did_pval = model.pvalues.get('post:treat', np.nan)
        print(f"  DID 효과: {did_coef:,.0f} (p={did_pval:.4f})")
        return {
            'broadcast': broadcast_name,
            'did_effect': did_coef,
            'p_value': did_pval,
            'significant': did_pval < 0.05 if not np.isnan(did_pval) else False,
            'r_squared': model.rsquared,
        }
    except ImportError:
        # statsmodels 없으면 수동 DID
        try:
            treat_pre = summary.loc[0, 1] if 1 in summary.columns else 0
            treat_post = summary.loc[1, 1] if 1 in summary.columns else 0
            ctrl_pre = summary.loc[0, 0] if 0 in summary.columns else 0
            ctrl_post = summary.loc[1, 0] if 0 in summary.columns else 0
            did = (treat_post - treat_pre) - (ctrl_post - ctrl_pre)
            print(f"  DID 효과 (수동): {did:,.0f}")
            return {
                'broadcast': broadcast_name,
                'did_effect': did,
                'p_value': np.nan,
                'significant': 'unknown',
                'r_squared': np.nan,
            }
        except:
            return None


if len(df_did_panel) > 0:
    did_results = []
    for b in BROADCASTS:
        air_ym = pd.Timestamp(b['air_date']).strftime('%Y%m')
        result = run_did(
            df_did_panel, 'CRTR_YM', 'outsider_amt', 'group',
            treatment_time=int(air_ym),
            broadcast_name=b['name']
        )
        if result:
            did_results.append(result)

    if did_results:
        df_did_results = pd.DataFrame(did_results)
        df_did_results.to_csv(OUTPUT_DIR / "did_results.csv", index=False, encoding='utf-8-sig')
        print(f"\n  저장: did_results.csv")
        print(df_did_results.to_string(index=False))
else:
    print("  DID 패널 데이터 없음")


# ============================================================
# 4. CausalImpact (Google 인과추론)
# ============================================================
print("\n--- [방법3] CausalImpact ---")

def run_causal_impact(y_series, intervention_date, pre_start=None):
    """
    CausalImpact: 합성대조군으로 "방송 없었으면" 예측 → 실제와 차이
    """
    try:
        from causalimpact import CausalImpact
    except ImportError:
        print("  [!] causalimpact 설치 필요: pip install causalimpact")
        return None

    y = y_series.copy().sort_index()
    intervention = pd.Timestamp(intervention_date)

    if pre_start is None:
        pre_start = y.index.min()

    pre_period = [pre_start, intervention - pd.Timedelta(days=1)]
    post_period = [intervention, min(intervention + pd.Timedelta(days=WINDOW_POST), y.index.max())]

    if pre_period[1] <= pre_period[0] or post_period[1] <= post_period[0]:
        return None

    data = pd.DataFrame({'y': y})
    try:
        ci = CausalImpact(data, pre_period, post_period)
        return ci
    except Exception as e:
        print(f"  CausalImpact 실패: {e}")
        return None

# T맵 관광지별 CausalImpact
if len(df_tmap_poi) > 0:
    ci_results = []
    for b in BROADCASTS:
        air = pd.Timestamp(b['air_date'])

        # 해당 방송의 관광지 필터
        poi_matches = []
        for loc in b['locations']:
            for poi in BROADCAST_POI_KEYWORDS:
                if poi in loc or any(kw in loc for kw in BROADCAST_POI_KEYWORDS.get(poi, [])):
                    poi_matches.append(poi)

        if not poi_matches:
            continue

        for poi in set(poi_matches):
            ts = df_tmap_poi[df_tmap_poi['matched_poi'] == poi].set_index('drv_ymd')['visit_cnt']
            ts = ts.resample('D').sum().fillna(0)

            if len(ts) < 60:
                continue

            ci = run_causal_impact(ts, b['air_date'])
            if ci:
                summary = ci.summary_data
                ci_results.append({
                    'broadcast': b['name'],
                    'poi': poi,
                    'air_date': b['air_date'],
                    'avg_actual': summary['average']['actual'].get('post', np.nan) if hasattr(summary, 'get') else np.nan,
                    'avg_predicted': summary['average']['predicted'].get('post', np.nan) if hasattr(summary, 'get') else np.nan,
                    'avg_effect': summary['average']['abs_effect'].get('post', np.nan) if hasattr(summary, 'get') else np.nan,
                    'cumulative_effect': summary['cumulative']['abs_effect'].get('post', np.nan) if hasattr(summary, 'get') else np.nan,
                })
                print(f"  [{b['name']}] {poi}: CausalImpact 완료")

    if ci_results:
        df_ci = pd.DataFrame(ci_results)
        df_ci.to_csv(OUTPUT_DIR / "causal_impact_results.csv", index=False, encoding='utf-8-sig')
        print(f"\n  저장: causal_impact_results.csv")


# ============================================================
# 5. 교란변수 통제 체크리스트
# ============================================================
print("\n--- 교란변수 통제 상태 ---")
print("""
| 교란변수         | 통제 방법                    | 상태  |
|----------------|--------------------------|------|
| 계절성 (월별)     | STL 분해 → 잔차 분석           | 완료  |
| 공휴일/연휴       | 공휴일 더미변수 (utils.py)      | 준비됨 |
| 축제 동시개최      | DID에서 처치/대조 분리           | 완료  |
| 전국 관광 트렌드    | 비교도시(천안/서산) 데이터 필요     | 추가필요 |
| 날씨            | 기상청 API 연동 필요            | 미구현 |
| 요일 효과        | 주별 데이터로 요일 보정 가능        | 준비됨 |
| 방송 중복 효과     | 11월 집중방영 → 개별 분리 어려움    | 주의필요 |
""")

print("\n" + "=" * 60)
print("Step 3 완료!")
print("=" * 60)
