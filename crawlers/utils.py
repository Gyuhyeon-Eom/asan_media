"""유틸리티 함수."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pandas as pd

from models import ContentItem, TrendPoint


def setup_logging(level: int = logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("asan_crawler.log", encoding="utf-8"),
        ],
    )


def items_to_dataframe(items: list[ContentItem]) -> pd.DataFrame:
    """ContentItem 리스트 → DataFrame."""
    if not items:
        return pd.DataFrame()
    rows = []
    for it in items:
        rows.append({
            "platform": it.platform.value if it.platform else "",
            "keyword": it.keyword,
            "title": it.title,
            "url": it.url,
            "description": it.description[:200] if it.description else "",
            "author": it.author,
            "published_at": it.published_at,
            "view_count": it.view_count,
            "like_count": it.like_count,
            "comment_count": it.comment_count,
            "engagement": it.engagement,
            "tourism_category": it.tourism_category.value if it.tourism_category else "",
            "mentioned_spots": ",".join(it.mentioned_spots) if it.mentioned_spots else "",
            "is_review": it.is_review,
        })
    return pd.DataFrame(rows)


def trends_to_dataframe(trends: list[TrendPoint]) -> pd.DataFrame:
    """TrendPoint 리스트 → DataFrame."""
    if not trends:
        return pd.DataFrame()
    rows = []
    for t in trends:
        rows.append({
            "platform": t.platform.value if t.platform else "",
            "keyword": t.keyword,
            "timestamp": t.timestamp,
            "value": t.value,
            "metric_name": t.metric_name,
        })
    return pd.DataFrame(rows)


def compute_broadcast_impact(
    items_df: pd.DataFrame,
    broadcast_date: datetime,
    days_before: int = 7,
    days_after: int = 14,
) -> dict:
    """방송 전후 콘텐츠 지표 비교."""
    if items_df.empty or "published_at" not in items_df.columns:
        return {"error": "데이터 없음"}

    df = items_df.copy()
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    df = df.dropna(subset=["published_at"])

    bd = pd.Timestamp(broadcast_date)
    before_mask = (df["published_at"] >= bd - pd.Timedelta(days=days_before)) & (df["published_at"] < bd)
    after_mask = (df["published_at"] >= bd) & (df["published_at"] <= bd + pd.Timedelta(days=days_after))

    before = df[before_mask]
    after = df[after_mask]

    def _metrics(subset: pd.DataFrame) -> dict:
        return {
            "post_count": len(subset),
            "view_count": int(subset["view_count"].sum()) if "view_count" in subset else 0,
            "like_count": int(subset["like_count"].sum()) if "like_count" in subset else 0,
            "comment_count": int(subset["comment_count"].sum()) if "comment_count" in subset else 0,
            "engagement_score": int(subset["engagement"].sum()) if "engagement" in subset else 0,
        }

    def _lift(before_m: dict, after_m: dict) -> dict:
        lift = {}
        for k in before_m:
            bv = before_m[k]
            av = after_m[k]
            if bv > 0:
                lift[k] = round((av - bv) / bv * 100, 1)
            else:
                lift[k] = float("inf") if av > 0 else 0
        return lift

    before_m = _metrics(before)
    after_m = _metrics(after)

    result = {
        "overall": {
            "before": before_m,
            "after": after_m,
            "lift_pct": _lift(before_m, after_m),
        },
    }

    # 카테고리별
    if "tourism_category" in df.columns:
        by_cat = {}
        for cat in df["tourism_category"].dropna().unique():
            if not cat:
                continue
            cat_before = _metrics(before[before["tourism_category"] == cat])
            cat_after = _metrics(after[after["tourism_category"] == cat])
            by_cat[cat] = {
                "before": cat_before,
                "after": cat_after,
                "lift_pct": _lift(cat_before, cat_after),
            }
        result["by_category"] = by_cat

    # 관광지 언급 순위
    if "mentioned_spots" in after.columns:
        spots = []
        for s in after["mentioned_spots"].dropna():
            spots.extend([x.strip() for x in s.split(",") if x.strip()])
        from collections import Counter
        result["top_spots"] = dict(Counter(spots).most_common(10))

    # 후기 증가율
    if "is_review" in df.columns:
        before_reviews = before["is_review"].sum()
        after_reviews = after["is_review"].sum()
        if before_reviews > 0:
            result["review_surge_pct"] = round((after_reviews - before_reviews) / before_reviews * 100, 1)
        elif after_reviews > 0:
            result["review_surge_pct"] = float("inf")

    # 피크 날짜
    if len(after) > 0:
        daily = after.groupby(after["published_at"].dt.date).size()
        if len(daily) > 0:
            result["peak_date"] = str(daily.idxmax())

    return result
