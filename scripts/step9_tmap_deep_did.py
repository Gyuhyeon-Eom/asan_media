"""
Step 9: T맵 심층 DID 분석 (전년 동기 비교 + 확장 윈도우)
======================================================
- 2025-01~03 vs 2026-01~03 전년 동기 비교
- 2025-12 포함한 확장 DID (방송 전 42일)
- 뛰어야산다2 집중 분석

실행: python step9_tmap_deep_did.py
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import platform
from pathlib import Path

if platform.system() == "Darwin":
    plt.rcParams["font.family"] = "AppleGothic"
elif platform.system() == "Windows":
    plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False

from config import BROADCASTS, OUTPUT_DIR

REPORT_DIR = OUTPUT_DIR / "report"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 60)
print("Step 9: T맵 심층 DID 분석")
print("=" * 60)

# ============================================================
# 1. T맵 데이터 로드
# ============================================================

INBOUND = Path("/Users/eomgyuhyeon/.openclaw/media/inbound")

tmap_files = {
    "2025-01": INBOUND / "4f04101b-ae3c-4f92-b99f-ca5a8dac04fb.csv",
    "2025-02": INBOUND / "c8269447-3eb5-4418-aec1-faca498dce7d.csv",
    "2025-03": INBOUND / "1ded847e-9734-4bdc-9da9-662c393290f1.csv",
    "2025-04": INBOUND / "5fd1759a-7eed-45df-a0d4-736e7df21fe7.csv",
    "2025-11": INBOUND / "38c4cf8d-2241-4447-8176-22935c9def22.csv",
    "2025-12": INBOUND / "d8cac458-ac19-47f9-b8a4-0ed68ae7ce04.csv",
    "2026-01": INBOUND / "28ec64fd-a593-42cc-9530-1fc2d2a1b441.csv",
    "2026-02": INBOUND / "aab2bac5-2d74-419c-848a-b4afd07914c2.csv",
    "2026-03": INBOUND / "7c46f8fc-aa33-4548-8268-663c622058dd.csv",
}

all_dfs = []
for label, fpath in tmap_files.items():
    if not fpath.exists():
        print(f"  [!] {label} 파일 없음: {fpath}")
        continue
    df = pd.read_csv(fpath, encoding="utf-8-sig")
    # 컬럼명 소문자 통일
    df.columns = [c.lower() for c in df.columns]
    df["month_label"] = label
    all_dfs.append(df)
    print(f"  {label}: {len(df):,}행")

df_all = pd.concat(all_dfs, ignore_index=True)
print(f"\n  전체: {len(df_all):,}행")

# 날짜 파싱
df_all["date"] = pd.to_datetime(df_all["drv_ymd"].astype(str), format="%Y%m%d")
df_all["year"] = df_all["date"].dt.year
df_all["month"] = df_all["date"].dt.month
df_all["dayofweek"] = df_all["date"].dt.dayofweek

# ============================================================
# 2. 관광지 분류 (처치군/대조군)
# ============================================================

# 뛰어야산다2 노출 관광지 키워드
TREAT_KEYWORDS = ["신정호", "곡교천", "현충사", "온양온천"]
CONTROL_KEYWORDS = ["도고온천", "도고", "외암민속마을", "외암", "피나클랜드"]

def classify_poi(name):
    name = str(name)
    for kw in TREAT_KEYWORDS:
        if kw in name:
            return "treat"
    for kw in CONTROL_KEYWORDS:
        if kw in name:
            return "control"
    return "other"

df_all["group"] = df_all["dstn_nm"].apply(classify_poi)
df_tourism = df_all[df_all["group"].isin(["treat", "control"])].copy()
print(f"\n  관광지 필터: {len(df_tourism):,}행 (처치+대조)")
print(f"  처치군: {(df_tourism['group'] == 'treat').sum():,}행")
print(f"  대조군: {(df_tourism['group'] == 'control').sum():,}행")

# ============================================================
# 3. 전년 동기 비교 (2025-01~03 vs 2026-01~03)
# ============================================================

print("\n--- 전년 동기 비교 (2025 vs 2026, 1~3월) ---")

df_q1 = df_tourism[df_tourism["month"].isin([1, 2, 3])].copy()

daily_q1 = df_q1.groupby(["date", "year", "group"]).size().reset_index(name="visits")

summary_yoy = daily_q1.groupby(["year", "group"]).agg(
    avg_daily=("visits", "mean"),
    total=("visits", "sum"),
    days=("visits", "count"),
).reset_index()

print(summary_yoy.to_string(index=False))

# 전년 대비 변화율
for grp in ["treat", "control"]:
    y25 = summary_yoy[(summary_yoy["year"] == 2025) & (summary_yoy["group"] == grp)]
    y26 = summary_yoy[(summary_yoy["year"] == 2026) & (summary_yoy["group"] == grp)]
    if len(y25) > 0 and len(y26) > 0:
        pct = (y26.iloc[0]["avg_daily"] - y25.iloc[0]["avg_daily"]) / y25.iloc[0]["avg_daily"] * 100
        print(f"  {grp}: 2025 {y25.iloc[0]['avg_daily']:.1f} → 2026 {y26.iloc[0]['avg_daily']:.1f} ({pct:+.1f}%)")

summary_yoy.to_csv(OUTPUT_DIR / "tmap_yoy_q1_summary.csv", index=False, encoding="utf-8-sig")

# ============================================================
# 4. 확장 DID: 2025-12 포함 (방송 전 42일)
# ============================================================

print("\n--- 확장 DID (2025-12-01 ~ 2026-03-31) ---")

air_date = pd.to_datetime("2026-01-12")
df_extended = df_tourism[
    (df_tourism["date"] >= "2025-12-01") &
    (df_tourism["date"] <= "2026-03-31")
].copy()

df_extended["post"] = (df_extended["date"] >= air_date).astype(int)
df_extended["treat"] = (df_extended["group"] == "treat").astype(int)

daily_ext = df_extended.groupby(["date", "group", "post", "treat"]).size().reset_index(name="visits")
daily_ext["dayofweek"] = pd.to_datetime(daily_ext["date"]).dt.dayofweek
daily_ext["is_weekend"] = (daily_ext["dayofweek"] >= 5).astype(int)

# DID 요약
did_summary = daily_ext.groupby(["group", "post"]).agg(
    avg_visits=("visits", "mean"),
    total_visits=("visits", "sum"),
    days=("visits", "count"),
).reset_index()
did_summary["period"] = did_summary["post"].map({0: "방송 전", 1: "방송 후"})
print(did_summary[["group", "period", "avg_visits", "days"]].to_string(index=False))

# DID 계산
treat_pre = did_summary[(did_summary["group"] == "treat") & (did_summary["post"] == 0)]["avg_visits"].values[0]
treat_post = did_summary[(did_summary["group"] == "treat") & (did_summary["post"] == 1)]["avg_visits"].values[0]
ctrl_pre = did_summary[(did_summary["group"] == "control") & (did_summary["post"] == 0)]["avg_visits"].values[0]
ctrl_post = did_summary[(did_summary["group"] == "control") & (did_summary["post"] == 1)]["avg_visits"].values[0]

did_effect = (treat_post - treat_pre) - (ctrl_post - ctrl_pre)
print(f"\n  노출: {treat_pre:.1f} → {treat_post:.1f} ({treat_post - treat_pre:+.1f})")
print(f"  대조: {ctrl_pre:.1f} → {ctrl_post:.1f} ({ctrl_post - ctrl_pre:+.1f})")
print(f"  DID 효과: {did_effect:+.1f}건/일")

# 회귀분석 (요일 통제)
try:
    import statsmodels.formula.api as smf

    daily_ext["did"] = daily_ext["post"] * daily_ext["treat"]
    for dow in range(7):
        daily_ext[f"dow_{dow}"] = (daily_ext["dayofweek"] == dow).astype(int)

    # 모델 1: 단순 DID
    m1 = smf.ols("visits ~ post + treat + did", data=daily_ext).fit()
    print(f"\n  [모델1 단순 DID] did={m1.params['did']:.2f}, p={m1.pvalues['did']:.4f}, R²={m1.rsquared:.3f}")

    # 모델 2: +요일 통제
    m2 = smf.ols("visits ~ post + treat + did + dow_0 + dow_1 + dow_2 + dow_3 + dow_4 + dow_5",
                  data=daily_ext).fit()
    print(f"  [모델2 +요일] did={m2.params['did']:.2f}, p={m2.pvalues['did']:.4f}, R²={m2.rsquared:.3f}")

    # 모델 3: +주말
    m3 = smf.ols("visits ~ post + treat + did + is_weekend", data=daily_ext).fit()
    print(f"  [모델3 +주말] did={m3.params['did']:.2f}, p={m3.pvalues['did']:.4f}, R²={m3.rsquared:.3f}")

    # 결과 저장
    reg_results = pd.DataFrame({
        "model": ["단순 DID", "+요일 통제", "+주말 통제"],
        "did_effect": [m1.params["did"], m2.params["did"], m3.params["did"]],
        "p_value": [m1.pvalues["did"], m2.pvalues["did"], m3.pvalues["did"]],
        "r_squared": [m1.rsquared, m2.rsquared, m3.rsquared],
        "pre_days": [42, 42, 42],
        "nobs": [m1.nobs, m2.nobs, m3.nobs],
    })
    reg_results.to_csv(OUTPUT_DIR / "tmap_extended_did_regression.csv", index=False, encoding="utf-8-sig")
    print(f"\n  회귀 결과 저장 완료")

except ImportError:
    print("  [!] statsmodels 미설치")

# ============================================================
# 5. 개별 관광지별 전년 동기 비교
# ============================================================

print("\n--- 개별 관광지 전년 동기 비교 (1~3월) ---")

# 주요 관광지별 집계
poi_keywords = {
    "신정호": "treat", "곡교천": "treat", "현충사": "treat", "온양온천": "treat",
    "도고온천": "control", "외암민속마을": "control", "피나클랜드": "control",
}

def match_poi(name):
    for kw in poi_keywords:
        if kw in str(name):
            return kw
    return None

df_q1_all = df_all[df_all["month"].isin([1, 2, 3])].copy()
df_q1_all["poi"] = df_q1_all["dstn_nm"].apply(match_poi)
df_q1_poi = df_q1_all[df_q1_all["poi"].notna()]

poi_yoy = df_q1_poi.groupby(["poi", "year"]).agg(
    total=("poi", "count"),
).reset_index()

# pivot
poi_pivot = poi_yoy.pivot(index="poi", columns="year", values="total").fillna(0)
if 2025 in poi_pivot.columns and 2026 in poi_pivot.columns:
    poi_pivot["change_pct"] = ((poi_pivot[2026] - poi_pivot[2025]) / poi_pivot[2025] * 100).round(1)
    poi_pivot["group"] = poi_pivot.index.map(lambda x: poi_keywords.get(x, "other"))
    print(poi_pivot.to_string())
    poi_pivot.to_csv(OUTPUT_DIR / "tmap_poi_yoy_comparison.csv", encoding="utf-8-sig")

# ============================================================
# 6. 시각화
# ============================================================

print("\n--- 시각화 ---")

# 차트 1: 전년 동기 비교 (월별 일평균)
fig, axes = plt.subplots(1, 2, figsize=(13, 5))

for idx, grp in enumerate(["treat", "control"]):
    ax = axes[idx]
    sub = daily_q1[daily_q1["group"] == grp].copy()
    sub["month"] = pd.to_datetime(sub["date"]).dt.month
    monthly_avg = sub.groupby(["year", "month"]).agg(avg=("visits", "mean")).reset_index()

    for year in [2025, 2026]:
        ys = monthly_avg[monthly_avg["year"] == year]
        label = str(year)
        ls = "--" if year == 2025 else "-"
        lw = 1 if year == 2025 else 2
        ax.plot(ys["month"], ys["avg"], marker="o", ls=ls, lw=lw, label=label)

    title = "노출 관광지" if grp == "treat" else "대조 관광지"
    ax.set_title(f"{title} (T맵 일평균 방문)")
    ax.set_xlabel("월")
    ax.set_ylabel("일평균 방문건수")
    ax.set_xticks([1, 2, 3])
    ax.legend()
    ax.grid(True, alpha=0.3)

fig.suptitle("전년 동기 비교: 2025 Q1 vs 2026 Q1", fontsize=14, fontweight="bold")
plt.tight_layout()
yoy_path = REPORT_DIR / "chart_tmap_yoy.png"
plt.savefig(yoy_path, dpi=150, bbox_inches="tight")
plt.close()
print(f"  전년 동기 차트: OK")

# 차트 2: 확장 DID 시계열 (2025-12 ~ 2026-03)
fig, ax = plt.subplots(figsize=(14, 5))
for grp, color, label in [("treat", "#e74c3c", "노출 관광지"), ("control", "#3498db", "대조 관광지")]:
    sub = daily_ext[daily_ext["group"] == grp].sort_values("date")
    ax.plot(sub["date"], sub["visits"], color=color, alpha=0.4, lw=0.8)
    # 7일 이동평균
    sub_ma = sub.set_index("date")["visits"].rolling(7, center=True).mean()
    ax.plot(sub_ma.index, sub_ma.values, color=color, lw=2, label=f"{label} (7일 MA)")

ax.axvline(air_date, color="black", ls="--", lw=1.5, label="방영일 (1/12)")
ax.set_ylabel("일별 방문건수")
ax.set_title("확장 DID: 2025-12 ~ 2026-03 (방송 전 42일)", fontsize=13)
ax.legend(fontsize=9)
ax.grid(True, alpha=0.3)
plt.tight_layout()
ext_path = REPORT_DIR / "chart_tmap_extended_did.png"
plt.savefig(ext_path, dpi=150, bbox_inches="tight")
plt.close()
print(f"  확장 DID 시계열: OK")

# 차트 3: 개별 관광지 전년 동기
if 2025 in poi_pivot.columns and 2026 in poi_pivot.columns:
    fig, ax = plt.subplots(figsize=(10, 5))
    pois = poi_pivot.sort_values(2026, ascending=True).index
    y = range(len(pois))
    w = 0.35
    ax.barh([i - w/2 for i in y], [poi_pivot.loc[p, 2025] for p in pois],
            height=w, label="2025 Q1", color="#3498db", alpha=0.8)
    ax.barh([i + w/2 for i in y], [poi_pivot.loc[p, 2026] for p in pois],
            height=w, label="2026 Q1", color="#e74c3c", alpha=0.8)
    ax.set_yticks(list(y))
    ax.set_yticklabels(pois, fontsize=9)
    ax.set_xlabel("총 방문건수 (Q1)")
    ax.set_title("개별 관광지 전년 동기 비교 (2025 vs 2026 Q1)")
    ax.legend()
    ax.grid(True, alpha=0.3, axis="x")
    plt.tight_layout()
    poi_path = REPORT_DIR / "chart_tmap_poi_yoy.png"
    plt.savefig(poi_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  관광지별 전년 동기: OK")

# ============================================================
# 7. 결과 요약
# ============================================================

did_summary.to_csv(OUTPUT_DIR / "tmap_extended_did_summary.csv", index=False, encoding="utf-8-sig")

print(f"\n{'=' * 60}")
print("핵심 결과:")
print(f"  1. 확장 DID (방송 전 42일): {did_effect:+.1f}건/일")
print(f"  2. 전년 동기 비교 (1~3월)")
for grp in ["treat", "control"]:
    y25 = summary_yoy[(summary_yoy["year"] == 2025) & (summary_yoy["group"] == grp)]
    y26 = summary_yoy[(summary_yoy["year"] == 2026) & (summary_yoy["group"] == grp)]
    if len(y25) > 0 and len(y26) > 0:
        pct = (y26.iloc[0]["avg_daily"] - y25.iloc[0]["avg_daily"]) / y25.iloc[0]["avg_daily"] * 100
        print(f"     {grp}: {pct:+.1f}%")
print(f"{'=' * 60}")
