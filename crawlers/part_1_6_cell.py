# ============================================================
# Part 1.6 — 검색량 수집·분석 (네이버 데이터랩 + YouTube 보조)
# 의존: 위 Part 0에서 정의된 bdf (방송 메타), search_volume.py
# 목적: ① 결과변수 교차검증(검색↔카드/T맵)  ② 11월 시점분리
# ============================================================
from search_volume import (load_env, naver_trends, spike_metrics, youtube_uploads)
load_env()  # .env에서 NAVER_*, YOUTUBE_API_KEY 로드

# ---- 방송별 검색 키워드 매핑 (메타 기반) ----
# 고유 키워드: 그 방송에만 반응 → 방송별 분리에 핵심
# 공통 키워드("아산 가볼만한곳")는 분리 불가라 앵커로만 사용
BROADCAST_KEYWORDS = {
    "전국노래자랑 아산시편":       ["전국노래자랑 아산", "아산 전국노래자랑"],
    "전현무계획2":                ["전현무계획 아산", "전현무계획2"],
    "굿모닝 대한민국":            ["굿모닝 대한민국 아산", "온양온천 굿모닝"],
    "6시 내고향":                ["6시 내고향 아산", "아산 6시내고향"],
    "박원숙의 같이삽시다 시즌3":   ["박원숙 같이삽시다 아산", "외암마을 박원숙"],
    "뛰어야산다2":               ["뛰어야산다 아산", "뛰어야산다2"],
    "황제성의 황제파워":          ["황제성 아산", "황제파워 온양"],
}
ANCHOR = {"groupName": "아산_앵커", "keywords": ["아산", "아산시", "아산 여행"]}

# ---- 수집 기간: 전체 방송을 포괄 ----
COLLECT_START = (bdf["air_date"].min() - pd.Timedelta(days=35)).date()
COLLECT_END   = (bdf["air_date"].max() + pd.Timedelta(days=21)).date()
print(f"수집 기간: {COLLECT_START} ~ {COLLECT_END}")

# ---- 네이버 데이터랩 수집 (7개 방송 → 앵커 재정규화로 단일 척도) ----
groups = [{"groupName": nm, "keywords": kws} for nm, kws in BROADCAST_KEYWORDS.items()]
try:
    nv = naver_trends(groups, COLLECT_START, COLLECT_END, anchor=ANCHOR, time_unit="date")
    print("✔ 네이버 데이터랩 수집:", nv.shape, "| 컬럼:", list(nv.columns))
    # [검증 1] 7개 방송 전부 키워드 할당 + 결측 구간 점검
    assert len(nv.columns) == len(BROADCAST_KEYWORDS), "방송-키워드 매핑 누락"
    print("  결측 비율(방송별):")
    print((nv.isna().mean().round(3)).to_string())
except Exception as e:
    print("✘ 네이버 수집 실패:", e)
    nv = pd.DataFrame()

# ---- [검증 2/3] 방송별 스파이크 측정: 고립 방송에서 명확한 lift = 측정 작동 증거 ----
spike_rows = []
if len(nv):
    for _, b in bdf.iterrows():
        nm = b["name"]
        if nm not in nv.columns:
            continue
        sm = spike_metrics(nv[nm], b["air_date"])
        spike_rows.append({"프로그램명": nm, "방영일": b["air_date"].date(), **sm})
spike_df = pd.DataFrame(spike_rows)
print("\n=== 방송별 검색 스파이크 (lift=방영후최대/방영전평균, 시차=피크까지 일수) ===")
display(spike_df)

# ---- [검증 4] 결과변수 교차검증: 검색 lift ↔ 카드/T맵 효과 부호·순위 비교 ----
# final_causal(셀 25) 또는 final(셀 40)의 효과값과 결합
def _effect_lookup():
    if "final_causal" in globals() and len(final_causal):
        return final_causal.set_index("프로그램명")["효과"]
    if "final" in globals() and len(final):
        return final.set_index("프로그램명")["효과값"]
    return pd.Series(dtype=float)

eff = _effect_lookup()
if len(spike_df) and len(eff):
    xchk = spike_df.set_index("프로그램명").join(eff.rename("인과효과"))
    xchk["검색_효과_동방향"] = (
        np.sign(xchk["lift"] - 1) == np.sign(xchk["인과효과"])
    )
    print("\n=== 교차검증: 검색 스파이크 vs 인과효과 ===")
    display(xchk[["lift", "peak_lag_days", "인과효과", "검색_효과_동방향"]])
    print("※ '동방향=True'면 검색 관심도와 실제 방문/소비 효과가 같은 방향 → 효과 신뢰 보강.")
else:
    print("\n(교차검증 보류: 인과효과 표 또는 검색 데이터 없음)")

# ---- [검증 5] 11월 시점분리: 겹친 방송의 고유 키워드가 각 방영일에 정렬되는지 ----
nov = [n for n in BROADCAST_KEYWORDS
       if n in (nv.columns if len(nv) else []) and bdf.set_index("name").loc[n, "air_date"].month == 11]
if len(nv) and len(nov) >= 2:
    print("\n=== 11월 겹침 방송 — 고유 키워드 피크 시점 분리 ===")
    sep = spike_df[spike_df["프로그램명"].isin(nov)][["프로그램명","방영일","peak_date","lift"]]
    display(sep.sort_values("방영일"))
    print("※ 각 방송 고유 키워드의 피크가 자기 방영일 근처(시차 작음)면 → 방송별 화제성 분리 성공.")
    print("  공통 키워드('아산 가볼만한곳')는 분리 불가이므로 여기 넣지 않음(설계상 한계).")

# ---- YouTube 보조: 11월 겹침 방송의 업로드 타이밍 ----
if os.environ.get("YOUTUBE_API_KEY"):
    print("\n=== YouTube 업로드 타이밍 (노출 대리지표, 보조) ===")
    for nm in nov:
        b = bdf.set_index("name").loc[nm]
        try:
            yt = youtube_uploads(BROADCAST_KEYWORDS[nm][0],
                                 b["air_date"] - pd.Timedelta(days=7),
                                 b["air_date"] + pd.Timedelta(days=21))
            print(f"  [{nm}] 영상 {len(yt)}건", 
                  f"| 첫 업로드 {yt['published_at'].min().date()}" if len(yt) else "| 없음")
        except Exception as e:
            print(f"  [{nm}] YouTube 조회 실패: {e}")
