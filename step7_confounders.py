"""
Step 7: 교란요소 수집 및 분석 (날씨/시즌/공휴일/이벤트)
=====================================================
- Open-Meteo API: 아산시 일별 기온/강수/적설/풍속
- 공휴일/요일/연휴 캘린더
- 계절성 인덱스 (전년 동기 대비)
- 교란요소 통제 회귀모델

실행: python step7_confounders.py
필요: pip install requests pandas numpy statsmodels
"""
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config import BROADCASTS, OUTPUT_DIR, WINDOW_PRE, WINDOW_POST

print("=" * 60)
print("Step 7: 교란요소 수집 및 분석")
print("=" * 60)

# 아산시 좌표
ASAN_LAT = 36.7898
ASAN_LON = 127.0018

# ============================================================
# 1. Open-Meteo 날씨 데이터 수집
# ============================================================

def fetch_weather(lat, lon, start_date, end_date):
    """Open-Meteo API로 일별 날씨 수집 (무료, API키 불필요)"""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join([
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "precipitation_sum",
            "snowfall_sum",
            "windspeed_10m_max",
            "weathercode",
        ]),
        "timezone": "Asia/Seoul",
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()["daily"]
        df = pd.DataFrame(data)
        df.rename(columns={"time": "date"}, inplace=True)
        df["date"] = pd.to_datetime(df["date"])
        return df
    except Exception as e:
        print(f"  [!] 날씨 API 에러: {e}")
        return pd.DataFrame()


print("\n--- 날씨 데이터 수집 ---")
# 전체 분석 기간 (첫 방송 4주 전 ~ 마지막 방송 8주 후)
all_air_dates = [pd.to_datetime(bc["air_date"]) for bc in BROADCASTS]
weather_start = (min(all_air_dates) - timedelta(days=60)).strftime("%Y-%m-%d")
weather_end = min(
    (max(all_air_dates) + timedelta(days=90)),
    datetime.now() - timedelta(days=5)  # Open-Meteo는 최근 며칠 미지원
).strftime("%Y-%m-%d")

print(f"  기간: {weather_start} ~ {weather_end}")
df_weather = fetch_weather(ASAN_LAT, ASAN_LON, weather_start, weather_end)
if len(df_weather) > 0:
    print(f"  → {len(df_weather)}일 수집")
    df_weather.to_csv(OUTPUT_DIR / "weather_daily.csv", index=False, encoding="utf-8-sig")
else:
    print("  [!] 날씨 데이터 수집 실패")

# ============================================================
# 2. 공휴일/요일/연휴/시즌 캘린더
# ============================================================

print("\n--- 공휴일/시즌 캘린더 생성 ---")

# 한국 공휴일 (2025-2026)
KR_HOLIDAYS = {
    # 2025
    "2025-01-01": "신정",
    "2025-01-28": "설연휴",
    "2025-01-29": "설날",
    "2025-01-30": "설연휴",
    "2025-03-01": "3.1절",
    "2025-05-05": "어린이날",
    "2025-05-06": "대체공휴일(석가탄신일)",
    "2025-06-06": "현충일",
    "2025-08-15": "광복절",
    "2025-10-03": "개천절",
    "2025-10-05": "추석연휴",
    "2025-10-06": "추석",
    "2025-10-07": "추석연휴",
    "2025-10-08": "대체공휴일(추석)",
    "2025-10-09": "한글날",
    "2025-12-25": "크리스마스",
    # 2026
    "2026-01-01": "신정",
    "2026-02-16": "설연휴",
    "2026-02-17": "설날",
    "2026-02-18": "설연휴",
    "2026-03-01": "3.1절",
    "2026-03-02": "대체공휴일(3.1절)",
    "2026-05-05": "어린이날",
    "2026-05-24": "석가탄신일",
    "2026-05-25": "대체공휴일(석가탄신일)",
    "2026-06-06": "현충일",
    "2026-08-15": "광복절",
    "2026-08-17": "대체공휴일(광복절)",
    "2026-09-24": "추석연휴",
    "2026-09-25": "추석",
    "2026-09-26": "추석연휴",
    "2026-10-03": "개천절",
    "2026-10-05": "대체공휴일(개천절)",
    "2026-10-09": "한글날",
    "2026-12-25": "크리스마스",
}

# 아산시 주요 지역 이벤트
ASAN_EVENTS = {
    "2025-04-25": "이순신축제 시작",
    "2025-04-26": "이순신축제",
    "2025-04-27": "이순신축제 종료",
    "2025-05-01": "신정호정원 개원",
    "2025-11-01": "가을단풍 피크",
    "2025-11-15": "가을단풍 피크",
    "2025-12-01": "온천시즌 시작",
    "2026-01-01": "겨울비수기",
    "2026-02-16": "설연휴(관광 피크)",
    "2026-02-17": "설연휴(관광 피크)",
    "2026-02-18": "설연휴(관광 피크)",
    "2026-04-25": "이순신축제 시작",
    "2026-04-26": "이순신축제",
    "2026-05-01": "이순신축제",
    "2026-05-02": "이순신축제+황제파워 촬영",
}

# 관광 시즌 정의
def get_season(date):
    month = date.month
    if month in [3, 4, 5]:
        return "봄(성수기)"
    elif month in [6, 7, 8]:
        return "여름"
    elif month in [9, 10, 11]:
        return "가을(성수기)"
    else:
        return "겨울(비수기)"


def get_season_score(date):
    """관광 시즌 점수 (1~5, 높을수록 성수기)"""
    month = date.month
    scores = {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 3,
              7: 3, 8: 3, 9: 4, 10: 5, 11: 4, 12: 2}
    return scores.get(month, 3)


# 전체 기간 캘린더 생성
date_range = pd.date_range(weather_start, weather_end, freq="D")
cal_rows = []
for d in date_range:
    d_str = d.strftime("%Y-%m-%d")
    cal_rows.append({
        "date": d,
        "dayofweek": d.dayofweek,  # 0=월 6=일
        "is_weekend": 1 if d.dayofweek >= 5 else 0,
        "is_holiday": 1 if d_str in KR_HOLIDAYS else 0,
        "holiday_name": KR_HOLIDAYS.get(d_str, ""),
        "is_event": 1 if d_str in ASAN_EVENTS else 0,
        "event_name": ASAN_EVENTS.get(d_str, ""),
        "season": get_season(d),
        "season_score": get_season_score(d),
        "month": d.month,
    })

df_calendar = pd.DataFrame(cal_rows)
df_calendar.to_csv(OUTPUT_DIR / "calendar_confounders.csv", index=False, encoding="utf-8-sig")
print(f"  → {len(df_calendar)}일 캘린더 생성")

# ============================================================
# 3. 교란요소 통합 테이블 (날씨 + 캘린더)
# ============================================================

print("\n--- 교란요소 통합 ---")

if len(df_weather) > 0:
    df_confounders = df_calendar.merge(df_weather, on="date", how="left")
else:
    df_confounders = df_calendar.copy()

# 파생변수
if "temperature_2m_mean" in df_confounders.columns:
    df_confounders["cold_wave"] = (df_confounders["temperature_2m_min"] < -10).astype(int)
    df_confounders["heavy_rain"] = (df_confounders["precipitation_sum"] > 30).astype(int)
    df_confounders["heavy_snow"] = (df_confounders["snowfall_sum"] > 5).astype(int)
    df_confounders["nice_weather"] = (
        (df_confounders["temperature_2m_mean"].between(10, 25)) &
        (df_confounders["precipitation_sum"] < 1) &
        (df_confounders["windspeed_10m_max"] < 30)
    ).astype(int)

df_confounders.to_csv(OUTPUT_DIR / "confounders_merged.csv", index=False, encoding="utf-8-sig")
print(f"  → {len(df_confounders)}행, {len(df_confounders.columns)}열")

# ============================================================
# 4. 방송별 교란요소 요약
# ============================================================

print("\n--- 방송별 교란요소 분석 ---")

confounder_summary = []
for bc in BROADCASTS:
    air = pd.to_datetime(bc["air_date"])
    pre_start = air - timedelta(days=WINDOW_PRE)
    post_end = air + timedelta(days=WINDOW_POST)

    pre_mask = (df_confounders["date"] >= pre_start) & (df_confounders["date"] < air)
    post_mask = (df_confounders["date"] >= air) & (df_confounders["date"] <= post_end)

    row = {"방송": bc["name"], "방영일": bc["air_date"]}

    for label, mask in [("pre", pre_mask), ("post", post_mask)]:
        sub = df_confounders[mask]
        row[f"{label}_days"] = len(sub)
        row[f"{label}_weekends"] = sub["is_weekend"].sum()
        row[f"{label}_holidays"] = sub["is_holiday"].sum()
        row[f"{label}_events"] = sub["is_event"].sum()
        row[f"{label}_season_score_avg"] = sub["season_score"].mean()

        if "temperature_2m_mean" in sub.columns:
            row[f"{label}_temp_mean"] = sub["temperature_2m_mean"].mean()
            row[f"{label}_precip_total"] = sub["precipitation_sum"].sum()
            row[f"{label}_snow_total"] = sub["snowfall_sum"].sum()
            row[f"{label}_cold_wave_days"] = sub.get("cold_wave", pd.Series([0])).sum()
            row[f"{label}_nice_days"] = sub.get("nice_weather", pd.Series([0])).sum()

    # 전후 차이
    if "pre_temp_mean" in row and "post_temp_mean" in row:
        row["temp_diff"] = row["post_temp_mean"] - row["pre_temp_mean"]
        row["nice_diff"] = row.get("post_nice_days", 0) - row.get("pre_nice_days", 0)

    # 주요 교란요소 메모
    confounders_list = bc.get("confounders", [])
    row["known_confounders"] = "; ".join(confounders_list) if confounders_list else "없음"

    confounder_summary.append(row)

    print(f"\n  {bc['name']} ({bc['air_date']})")
    if "pre_temp_mean" in row:
        print(f"    기온: 방송전 {row.get('pre_temp_mean', 0):.1f}C → 방송후 {row.get('post_temp_mean', 0):.1f}C (차이 {row.get('temp_diff', 0):+.1f}C)")
        print(f"    쾌적한 날: 방송전 {row.get('pre_nice_days', 0)}일 → 방송후 {row.get('post_nice_days', 0)}일")
    print(f"    주말: 방송전 {row.get('pre_weekends', 0)}일, 방송후 {row.get('post_weekends', 0)}일")
    print(f"    공휴일: 방송전 {row.get('pre_holidays', 0)}일, 방송후 {row.get('post_holidays', 0)}일")
    print(f"    교란요소: {row['known_confounders']}")

df_confounder_summary = pd.DataFrame(confounder_summary)
df_confounder_summary.to_csv(
    OUTPUT_DIR / "confounder_summary_by_broadcast.csv",
    index=False, encoding="utf-8-sig"
)

# ============================================================
# 5. 교란요소 영향도 판정 (방송별)
# ============================================================

print(f"\n\n{'=' * 60}")
print("교란요소 영향도 판정:")
print("=" * 60)

for _, row in df_confounder_summary.iterrows():
    print(f"\n{row['방송']} ({row['방영일']}):")
    risks = []

    # 기온 변화
    td = row.get("temp_diff", 0)
    if abs(td) > 10:
        risks.append(f"기온 급변({td:+.1f}C) - 계절 전환 효과 혼재 가능")
    elif abs(td) > 5:
        risks.append(f"기온 변화({td:+.1f}C) - 약간의 계절 효과 가능")

    # 공휴일 비대칭
    hpre = row.get("pre_holidays", 0)
    hpost = row.get("post_holidays", 0)
    if abs(hpost - hpre) >= 2:
        risks.append(f"공휴일 비대칭 (전 {hpre}일 vs 후 {hpost}일)")

    # 이벤트
    if row.get("pre_events", 0) > 0 or row.get("post_events", 0) > 0:
        risks.append(f"지역이벤트 존재 (전 {row.get('pre_events', 0)}건, 후 {row.get('post_events', 0)}건)")

    # 시즌 점수
    spre = row.get("pre_season_score_avg", 3)
    spost = row.get("post_season_score_avg", 3)
    if abs(spost - spre) >= 1:
        risks.append(f"시즌 변화 (전 {spre:.1f} → 후 {spost:.1f})")

    # 한파/폭설
    cw = row.get("post_cold_wave_days", 0)
    if cw > 3:
        risks.append(f"한파 {cw}일 - 방문 억제 요인")

    if row.get("known_confounders", "없음") != "없음":
        risks.append(f"기존 알려진: {row['known_confounders']}")

    if risks:
        for r in risks:
            print(f"  [!] {r}")
    else:
        print("  교란요소 영향 낮음 (클린 비교 가능)")

print(f"\n\n결과 저장 완료: {OUTPUT_DIR}")
