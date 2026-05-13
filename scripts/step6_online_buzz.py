"""
Step 6: 온라인 버즈 수집 (네이버 블로그/뉴스 + 유튜브)
====================================================
- 네이버 검색 API: 블로그/뉴스 게시물 수 & 트렌드
- 네이버 DataLab: 검색량 추이
- YouTube Data API: 영상 수 & 조회수 & 댓글
- 방송별 전후 비교

실행: python step6_online_buzz.py
필요: pip install requests python-dotenv google-api-python-client pandas
"""
import os
import json
import time
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

from config import BROADCASTS, OUTPUT_DIR

load_dotenv()

NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "")
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "")

print("=" * 60)
print("Step 6: 온라인 버즈 수집")
print("=" * 60)

# ============================================================
# 1. 네이버 검색 API - 블로그/뉴스 게시물 수
# ============================================================

def naver_search_count(query, search_type="blog", display=1):
    """네이버 검색 API로 총 검색결과 수 조회"""
    url = f"https://openapi.naver.com/v1/search/{search_type}.json"
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }
    params = {"query": query, "display": display, "sort": "date"}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json().get("total", 0)
    except Exception as e:
        print(f"  [!] 네이버 검색 에러 ({query}): {e}")
        return None


def naver_search_posts(query, search_type="blog", display=100):
    """네이버 검색 API로 게시물 리스트 조회 (날짜 포함)"""
    url = f"https://openapi.naver.com/v1/search/{search_type}.json"
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }
    all_items = []
    for start in range(1, 1001, display):  # 최대 1000건
        params = {"query": query, "display": display, "start": start, "sort": "date"}
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("items", [])
            if not items:
                break
            all_items.extend(items)
            time.sleep(0.15)  # rate limit
        except Exception as e:
            print(f"  [!] 에러 (start={start}): {e}")
            break
    return all_items


def collect_naver_blog_timeline(keywords, date_from, date_to):
    """키워드별 블로그 게시물을 수집하고 일별 카운트 생성"""
    results = []
    for kw in keywords:
        print(f"  네이버 블로그 검색: '{kw}'")
        posts = naver_search_posts(kw, "blog", 100)
        for p in posts:
            # postdate: YYYYMMDD 형식
            pd_str = p.get("postdate", "")
            if pd_str:
                results.append({
                    "keyword": kw,
                    "date": pd_str,
                    "title": p.get("title", ""),
                    "link": p.get("link", ""),
                })
        print(f"    → {len(posts)}건 수집")
        time.sleep(0.5)

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    df = df[(df["date"] >= date_from) & (df["date"] <= date_to)]
    return df


# ============================================================
# 2. 네이버 DataLab - 검색 트렌드
# ============================================================

def naver_datalab_search(keywords, start_date, end_date, time_unit="date"):
    """네이버 DataLab 검색어 트렌드 API"""
    url = "https://openapi.naver.com/v1/datalab/search"
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
        "Content-Type": "application/json",
    }
    # 키워드를 5개씩 묶어서 (API 제한)
    all_results = []
    for i in range(0, len(keywords), 5):
        chunk = keywords[i:i+5]
        body = {
            "startDate": start_date,
            "endDate": end_date,
            "timeUnit": time_unit,
            "keywordGroups": [
                {"groupName": kw, "keywords": [kw]} for kw in chunk
            ],
        }
        try:
            resp = requests.post(url, headers=headers, json=body, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            for group in data.get("results", []):
                kw_name = group["title"]
                for item in group.get("data", []):
                    all_results.append({
                        "keyword": kw_name,
                        "date": item["period"],
                        "ratio": item["ratio"],
                    })
            time.sleep(0.3)
        except Exception as e:
            print(f"  [!] DataLab 에러: {e}")

    if not all_results:
        return pd.DataFrame()
    return pd.DataFrame(all_results)


# ============================================================
# 3. YouTube Data API - 영상 검색 & 댓글
# ============================================================

def youtube_search(query, published_after, published_before, max_results=500):
    """YouTube 영상 검색 (최대 500개까지 페이지네이션)"""
    if not YOUTUBE_API_KEY:
        print("  [!] YOUTUBE_API_KEY 미설정")
        return [], 0

    url = "https://www.googleapis.com/youtube/v3/search"
    all_items = []
    page_token = None
    total_results = 0

    while len(all_items) < max_results:
        params = {
            "part": "snippet",
            "q": query,
            "type": "video",
            "publishedAfter": published_after + "T00:00:00Z",
            "publishedBefore": published_before + "T23:59:59Z",
            "maxResults": min(50, max_results - len(all_items)),
            "order": "relevance",
            "key": YOUTUBE_API_KEY,
        }
        if page_token:
            params["pageToken"] = page_token

        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            page_info = data.get("pageInfo", {})
            if total_results == 0:
                total_results = page_info.get("totalResults", 0)
            items = data.get("items", [])
            all_items.extend(items)
            page_token = data.get("nextPageToken")
            if not page_token or not items:
                break
            time.sleep(0.2)
        except Exception as e:
            print(f"  [!] YouTube 검색 에러: {e}")
            break

    return all_items, total_results


def youtube_video_stats(video_ids):
    """YouTube 영상 통계 (조회수, 좋아요, 댓글수)"""
    if not YOUTUBE_API_KEY or not video_ids:
        return {}

    stats = {}
    # 50개씩 배치
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        url = "https://www.googleapis.com/youtube/v3/videos"
        params = {
            "part": "statistics,snippet",
            "id": ",".join(batch),
            "key": YOUTUBE_API_KEY,
        }
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            for item in resp.json().get("items", []):
                s = item.get("statistics", {})
                stats[item["id"]] = {
                    "title": item["snippet"]["title"],
                    "channel": item["snippet"]["channelTitle"],
                    "published": item["snippet"]["publishedAt"][:10],
                    "views": int(s.get("viewCount", 0)),
                    "likes": int(s.get("likeCount", 0)),
                    "comments": int(s.get("commentCount", 0)),
                }
            time.sleep(0.2)
        except Exception as e:
            print(f"  [!] YouTube 통계 에러: {e}")

    return stats


def youtube_comments(video_id, max_results=100):
    """YouTube 영상 댓글 수집"""
    if not YOUTUBE_API_KEY:
        return []

    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    comments = []
    page_token = None

    while len(comments) < max_results:
        params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": min(100, max_results - len(comments)),
            "order": "relevance",
            "key": YOUTUBE_API_KEY,
        }
        if page_token:
            params["pageToken"] = page_token

        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            for item in data.get("items", []):
                s = item["snippet"]["topLevelComment"]["snippet"]
                comments.append({
                    "video_id": video_id,
                    "author": s.get("authorDisplayName", ""),
                    "text": s.get("textDisplay", ""),
                    "published": s.get("publishedAt", "")[:10],
                    "likes": s.get("likeCount", 0),
                })
            page_token = data.get("nextPageToken")
            if not page_token:
                break
            time.sleep(0.2)
        except Exception as e:
            # 댓글 비활성화 등
            break

    return comments


# ============================================================
# 4. 방송별 수집 실행
# ============================================================

def collect_buzz_for_broadcast(bc):
    """한 방송에 대해 전후 온라인 버즈 수집"""
    name = bc["name"]
    air_date = pd.to_datetime(bc["air_date"])
    pre_start = air_date - timedelta(days=28)
    post_end = air_date + timedelta(days=56)  # 방송 후 8주

    pre_str = pre_start.strftime("%Y-%m-%d")
    post_str = post_end.strftime("%Y-%m-%d")

    print(f"\n--- {name} (방영일: {bc['air_date']}) ---")

    result = {"broadcast": name, "air_date": bc["air_date"]}

    # 검색 키워드
    base_keywords = [name, f"{name} 아산"]
    location_keywords = [f"아산 {loc}" for loc in bc.get("locations", [])[:3]]
    all_keywords = base_keywords + location_keywords + ["아산 여행", "아산 관광"]

    # --- 네이버 블로그 ---
    if NAVER_CLIENT_ID:
        print("  [네이버 블로그]")
        for kw in all_keywords[:5]:  # rate limit 고려
            total = naver_search_count(kw, "blog")
            if total is not None:
                result[f"naver_blog_{kw}"] = total
                print(f"    '{kw}': {total:,}건")
            time.sleep(0.2)

        # 블로그 게시물 일별 수집
        blog_df = collect_naver_blog_timeline(base_keywords[:2], pre_start, post_end)
        if len(blog_df) > 0:
            daily = blog_df.groupby(["keyword", blog_df["date"].dt.date]).size().reset_index(name="count")
            daily.columns = ["keyword", "date", "count"]
            result["blog_daily"] = daily

        # 네이버 뉴스
        print("  [네이버 뉴스]")
        for kw in base_keywords[:2]:
            total = naver_search_count(kw, "news")
            if total is not None:
                result[f"naver_news_{kw}"] = total
                print(f"    '{kw}': {total:,}건")
            time.sleep(0.2)

    # --- 네이버 DataLab ---
    if NAVER_CLIENT_ID:
        print("  [네이버 DataLab 검색 트렌드]")
        trend_df = naver_datalab_search(
            all_keywords[:5],
            pre_str, post_str, "date"
        )
        if len(trend_df) > 0:
            result["datalab_trend"] = trend_df
            print(f"    → {len(trend_df)}일 x {trend_df['keyword'].nunique()}키워드")

    # --- YouTube ---
    if YOUTUBE_API_KEY:
        print("  [YouTube]")
        # 방송 전후 영상 검색 (최대 500개 페이지네이션)
        for period, pstart, pend in [
            ("pre", pre_str, bc["air_date"]),
            ("post", bc["air_date"], post_str),
        ]:
            videos, yt_total = youtube_search(name, pstart, pend, max_results=500)
            video_ids = [v["id"]["videoId"] for v in videos if "videoId" in v.get("id", {})]

            if video_ids:
                stats = youtube_video_stats(video_ids)
                total_views = sum(s["views"] for s in stats.values())
                total_videos = len(stats)
                result[f"yt_{period}_videos"] = total_videos
                result[f"yt_{period}_total_estimated"] = yt_total  # API 추정 총건수
                result[f"yt_{period}_views"] = total_views
                print(f"    {period}: {total_videos}개 수집 (API추정 {yt_total}개), 총 {total_views:,} 조회")

                # 상위 5개 영상 댓글 수집
                top_videos = sorted(stats.items(), key=lambda x: x[1]["views"], reverse=True)[:5]
                all_comments = []
                for vid, vinfo in top_videos:
                    comments = youtube_comments(vid, max_results=50)
                    all_comments.extend(comments)
                result[f"yt_{period}_comments"] = all_comments
                print(f"    댓글 {len(all_comments)}건 수집")
            else:
                result[f"yt_{period}_videos"] = 0
                result[f"yt_{period}_total_estimated"] = yt_total
                result[f"yt_{period}_views"] = 0

            time.sleep(0.5)

        # "아산 여행" 일반 키워드도
        asan_videos, _ = youtube_search("아산 여행", pre_str, post_str, max_results=500)
        asan_ids = [v["id"]["videoId"] for v in asan_videos if "videoId" in v.get("id", {})]
        if asan_ids:
            asan_stats = youtube_video_stats(asan_ids)
            # 일별 업로드 카운트
            yt_daily = []
            for vid, s in asan_stats.items():
                yt_daily.append({"date": s["published"], "views": s["views"]})
            if yt_daily:
                result["yt_asan_daily"] = pd.DataFrame(yt_daily)
                print(f"    '아산 여행' 관련 영상: {len(asan_stats)}개")

    return result


# ============================================================
# 5. 전체 실행 & 저장
# ============================================================

all_buzz = []
summary_rows = []

for bc in BROADCASTS:
    buzz = collect_buzz_for_broadcast(bc)
    all_buzz.append(buzz)

    # 요약 행
    row = {
        "방송": buzz["broadcast"],
        "방영일": buzz["air_date"],
    }
    # 네이버 블로그/뉴스 총건수
    for k, v in buzz.items():
        if k.startswith("naver_") and isinstance(v, (int, float)):
            row[k] = v
    # YouTube
    for period in ["pre", "post"]:
        row[f"yt_{period}_videos"] = buzz.get(f"yt_{period}_videos", 0)
        row[f"yt_{period}_total_estimated"] = buzz.get(f"yt_{period}_total_estimated", 0)
        row[f"yt_{period}_views"] = buzz.get(f"yt_{period}_views", 0)

    summary_rows.append(row)

    # 일별 데이터 저장
    blog_daily = buzz.get("blog_daily")
    if blog_daily is not None and len(blog_daily) > 0:
        safe_name = buzz["broadcast"].replace(" ", "_").replace("/", "_")
        blog_daily.to_csv(
            OUTPUT_DIR / f"blog_daily_{safe_name}.csv",
            index=False, encoding="utf-8-sig"
        )

    trend_df = buzz.get("datalab_trend")
    if trend_df is not None and len(trend_df) > 0:
        safe_name = buzz["broadcast"].replace(" ", "_").replace("/", "_")
        trend_df.to_csv(
            OUTPUT_DIR / f"datalab_trend_{safe_name}.csv",
            index=False, encoding="utf-8-sig"
        )

    # YouTube 댓글 저장
    for period in ["pre", "post"]:
        comments = buzz.get(f"yt_{period}_comments", [])
        if comments:
            safe_name = buzz["broadcast"].replace(" ", "_").replace("/", "_")
            pd.DataFrame(comments).to_csv(
                OUTPUT_DIR / f"yt_comments_{safe_name}_{period}.csv",
                index=False, encoding="utf-8-sig"
            )

# 요약 테이블 저장
df_summary = pd.DataFrame(summary_rows)
df_summary.to_csv(OUTPUT_DIR / "online_buzz_summary.csv", index=False, encoding="utf-8-sig")
print(f"\n\n{'=' * 60}")
print("온라인 버즈 요약:")
print(df_summary.to_string(index=False))
print(f"\n결과 저장: {OUTPUT_DIR}")
