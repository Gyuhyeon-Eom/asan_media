"""
Part 1.6 — 검색량 수집 (네이버 데이터랩 + YouTube 보조)
방송 효과의 '결과변수 교차검증 + 11월 시점분리'를 위한 검색 관심도 수집.

핵심 제약 (반드시 이해):
- 네이버 데이터랩 검색어트렌드 API는 절대량이 아니라 '조회기간 내 최댓값=100'인 상대값(ratio)만 반환.
  → 한 번의 호출에 묶인 키워드끼리만 같은 척도. 호출이 다르면 척도가 다름.
  → 그래서 (a) 한 호출에 최대 5개 그룹을 같이 넣고, (b) 5개 초과면 공통 앵커('아산')로 재정규화.
- YouTube 조회수는 누적값이라 '노출 타이밍' 대리지표로만 사용 (업로드일 분포 + 영상 수).

키는 .env에서만 읽음 (하드코딩 금지). 필요한 키:
  NAVER_CLIENT_ID, NAVER_CLIENT_SECRET, YOUTUBE_API_KEY
"""
import os, json, time
from datetime import timedelta
import pandas as pd
import numpy as np
import urllib.request

# ------------------------------------------------------------------
# 키 로드: .env가 있으면 읽고, 없으면 이미 export된 환경변수 사용
# ------------------------------------------------------------------
def load_env(dotenv_path=".env"):
    if os.path.exists(dotenv_path):
        for line in open(dotenv_path, encoding="utf-8"):
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# ==================================================================
# 1. 네이버 데이터랩 검색어트렌드
# ==================================================================
NAVER_URL = "https://openapi.naver.com/v1/datalab/search"

def _naver_call(keyword_groups, start_date, end_date, time_unit="date"):
    """keyword_groups: [{'groupName': str, 'keywords': [str,...]}, ...]  (최대 5)
    반환: long-form DataFrame [groupName, date, ratio]"""
    cid = os.environ.get("NAVER_CLIENT_ID")
    csec = os.environ.get("NAVER_CLIENT_SECRET")
    if not cid or not csec:
        raise RuntimeError("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 가 환경변수에 없습니다 (.env 확인).")
    if len(keyword_groups) > 5:
        raise ValueError("데이터랩은 호출당 최대 5개 그룹. 묶거나 앵커 재정규화 필요.")
    body = json.dumps({
        "startDate": str(start_date), "endDate": str(end_date),
        "timeUnit": time_unit, "keywordGroups": keyword_groups,
    }).encode("utf-8")
    req = urllib.request.Request(NAVER_URL, data=body)
    req.add_header("X-Naver-Client-Id", cid)
    req.add_header("X-Naver-Client-Secret", csec)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as resp:
        out = json.loads(resp.read().decode("utf-8"))
    rows = []
    for r in out.get("results", []):
        g = r["title"]
        for pt in r["data"]:
            rows.append({"groupName": g, "date": pt["period"], "ratio": pt["ratio"]})
    df = pd.DataFrame(rows)
    if len(df):
        df["date"] = pd.to_datetime(df["date"])
    return df


def naver_trends(keyword_groups, start_date, end_date,
                 anchor=None, time_unit="date", sleep=0.3):
    """5개 초과 그룹을 앵커 키워드로 재정규화하여 단일 척도로 합침.
    anchor: 모든 청크에 공통으로 넣어 척도를 맞출 그룹 dict
            (예: {'groupName':'_anchor','keywords':['아산','아산시']})
            None이면 그룹이 5개 이하라고 가정하고 그대로 1회 호출.
    반환: pivot DataFrame (index=date, columns=groupName, values=ratio[앵커 기준 보정])"""
    if len(keyword_groups) <= 5 and anchor is None:
        df = _naver_call(keyword_groups, start_date, end_date, time_unit)
        return df.pivot(index="date", columns="groupName", values="ratio").sort_index()

    if anchor is None:
        raise ValueError("그룹 5개 초과 시 anchor 그룹이 필요합니다 (재정규화 기준).")

    # 앵커 + 최대 4개씩 청크. 청크별로 앵커 평균을 1.0으로 맞춰 곱셈 보정.
    chunks = [keyword_groups[i:i+4] for i in range(0, len(keyword_groups), 4)]
    merged = None
    for ch in chunks:
        df = _naver_call([anchor] + ch, start_date, end_date, time_unit)
        if df.empty:
            continue
        piv = df.pivot(index="date", columns="groupName", values="ratio").sort_index()
        a = piv[anchor["groupName"]].replace(0, np.nan)
        scale = a.mean()
        if scale and not np.isnan(scale):
            piv = piv.div(a / scale, axis=0)  # 앵커를 척도로 정규화
        piv = piv.drop(columns=[anchor["groupName"]])
        merged = piv if merged is None else merged.join(piv, how="outer")
        time.sleep(sleep)
    return merged.sort_index()


# ==================================================================
# 2. 스파이크 측정 (방영일 전후 비율 + 피크 시차)
# ==================================================================
def spike_metrics(series, air_date, pre_days=28, post_days=14):
    """series: 일별 검색량 (index=date). 방영일 전후 변화 정량화.
    반환 dict: pre평균, post최대, lift배수, 피크일, 피크시차(일)"""
    s = series.dropna()
    air = pd.Timestamp(air_date)
    pre = s[(s.index >= air - timedelta(days=pre_days)) & (s.index < air)]
    post = s[(s.index >= air) & (s.index <= air + timedelta(days=post_days))]
    if len(pre) == 0 or len(post) == 0:
        return {"pre_mean": np.nan, "post_max": np.nan, "lift": np.nan,
                "peak_date": pd.NaT, "peak_lag_days": np.nan}
    pre_mean = pre.mean()
    peak_date = post.idxmax()
    return {
        "pre_mean": round(pre_mean, 2),
        "post_max": round(post.max(), 2),
        "lift": round(post.max() / pre_mean, 2) if pre_mean else np.nan,
        "peak_date": peak_date,
        "peak_lag_days": int((peak_date - air).days),
    }


# ==================================================================
# 3. YouTube 보조 (업로드 타이밍 대리지표)
# ==================================================================
def youtube_uploads(query, published_after, published_before, max_results=50):
    """방송 관련 영상의 업로드일 분포 + 조회수. 노출 타이밍 대리지표.
    반환: DataFrame [title, channel, published_at, video_id]"""
    key = os.environ.get("YOUTUBE_API_KEY")
    if not key:
        raise RuntimeError("YOUTUBE_API_KEY 가 환경변수에 없습니다 (.env 확인).")
    base = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "key": key, "q": query, "part": "snippet", "type": "video",
        "maxResults": min(max_results, 50), "order": "date",
        "publishedAfter": pd.Timestamp(published_after).strftime("%Y-%m-%dT00:00:00Z"),
        "publishedBefore": pd.Timestamp(published_before).strftime("%Y-%m-%dT00:00:00Z"),
    }
    url = base + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=30) as resp:
        out = json.loads(resp.read().decode("utf-8"))
    rows = [{
        "title": it["snippet"]["title"],
        "channel": it["snippet"]["channelTitle"],
        "published_at": it["snippet"]["publishedAt"],
        "video_id": it["id"]["videoId"],
    } for it in out.get("items", [])]
    df = pd.DataFrame(rows)
    if len(df):
        df["published_at"] = pd.to_datetime(df["published_at"]).dt.tz_localize(None)
    return df
