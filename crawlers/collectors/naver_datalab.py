"""네이버 데이터랩 검색어 트렌드 수집기.

한국 시장에서 검색량 변화 추적에는 Google Trends보다
네이버 데이터랩이 훨씬 유의미한 데이터를 제공함.

API 문서: https://developers.naver.com/docs/serviceapi/datalab/search/search.md
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Optional

from models import CollectionResult, Platform, TrendPoint
from .base import BaseCollector

_DATALAB_URL = "https://openapi.naver.com/v1/datalab/search"


class NaverDatalabCollector(BaseCollector):
    """네이버 데이터랩 검색어 트렌드 수집기.

    - 네이버 Developers 동일 앱에서 데이터랩 API 사용 신청 필요
    - 일별 상대적 검색량(0~100) 제공
    - 최대 5개 키워드 그룹 동시 비교 가능
    - 기간: 최소 2016-01-01 ~ 현재
    """

    platform = Platform.NAVER_DATALAB
    rate_limit_per_sec = 5.0

    def __init__(self):
        super().__init__()
        self.client_id = os.getenv("NAVER_CLIENT_ID", "")
        self.client_secret = os.getenv("NAVER_CLIENT_SECRET", "")

    def _headers(self) -> dict:
        return {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
            "Content-Type": "application/json",
        }

    def collect(
        self,
        keyword: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_items: int = 100,
    ) -> CollectionResult:
        """단일 키워드 검색 트렌드 수집."""
        trends = self.get_trend(
            keywords=[keyword],
            start_date=start_date,
            end_date=end_date,
        )
        return CollectionResult(
            platform=Platform.NAVER_DATALAB,
            keyword=keyword,
            trends=trends,
            total_count=len(trends),
        )

    def get_trend(
        self,
        keywords: list[str],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        time_unit: str = "date",  # "date" | "week" | "month"
    ) -> list[TrendPoint]:
        """키워드 검색 트렌드 조회.

        Args:
            keywords: 검색 키워드 리스트 (최대 5개)
            time_unit: "date"(일별), "week"(주별), "month"(월별)
        """
        start_dt, end_dt = self.date_range_params(start_date, end_date, default_days_back=30)
        end_dt = min(end_dt, datetime.now())  # 미래 날짜 방지

        # 네이버 데이터랩은 keyword group 형태로 요청
        # 각 그룹에 동의어를 넣을 수 있음
        keyword_groups = []
        for kw in keywords[:5]:  # 최대 5그룹
            keyword_groups.append({
                "groupName": kw,
                "keywords": [kw],
            })

        body = {
            "startDate": start_dt.strftime("%Y-%m-%d"),
            "endDate": end_dt.strftime("%Y-%m-%d"),
            "timeUnit": time_unit,
            "keywordGroups": keyword_groups,
        }

        resp = self.post(_DATALAB_URL, json=body, headers=self._headers())
        data = resp.json()

        trends: list[TrendPoint] = []
        for result in data.get("results", []):
            group_name = result.get("title", "")
            for point in result.get("data", []):
                period = point.get("period", "")
                ratio = point.get("ratio", 0.0)
                try:
                    ts = datetime.strptime(period, "%Y-%m-%d")
                except ValueError:
                    continue

                trends.append(TrendPoint(
                    platform=Platform.NAVER_DATALAB,
                    keyword=group_name,
                    timestamp=ts,
                    value=ratio,
                    metric_name="search_trend_ratio",
                ))

        self.logger.info(
            f"[naver_datalab] {[k for k in keywords]} → {len(trends)}개 데이터 포인트"
        )
        return trends

    def compare_keywords(
        self,
        keywords: list[str],
        start_date: datetime,
        end_date: datetime,
        time_unit: str = "date",
    ) -> dict[str, list[TrendPoint]]:
        """여러 키워드 검색 트렌드 비교 (최대 5개).

        Usage:
            # 방송 전후 "아산 여행" vs "아산 맛집" vs "온양온천" 비교
            result = collector.compare_keywords(
                ["아산 여행", "아산 맛집", "온양온천"],
                start_date=datetime(2026, 4, 1),
                end_date=datetime(2026, 4, 20),
            )
        """
        trends = self.get_trend(keywords, start_date, end_date, time_unit)

        grouped: dict[str, list[TrendPoint]] = {}
        for t in trends:
            grouped.setdefault(t.keyword, []).append(t)

        return grouped

    def get_broadcast_impact_trend(
        self,
        keywords: list[str],
        broadcast_date: datetime,
        days_before: int = 7,
        days_after: int = 14,
    ) -> dict:
        """방송일 기준 전후 검색 트렌드 변화 분석.

        Returns:
            {
                "keyword": {
                    "before_avg": 방송 전 평균 검색량,
                    "after_avg": 방송 후 평균 검색량,
                    "peak_value": 최고 검색량,
                    "peak_date": 최고 검색량 날짜,
                    "lift_pct": 증가율(%),
                    "trend_data": [TrendPoint, ...],
                }
            }
        """
        start_date = broadcast_date - timedelta(days=days_before)
        end_date = min(broadcast_date + timedelta(days=days_after), datetime.now())

        grouped = self.compare_keywords(keywords, start_date, end_date)
        result = {}

        for kw, points in grouped.items():
            before = [p for p in points if p.timestamp < broadcast_date]
            after = [p for p in points if p.timestamp >= broadcast_date]

            before_avg = sum(p.value for p in before) / len(before) if before else 0
            after_avg = sum(p.value for p in after) / len(after) if after else 0

            peak = max(points, key=lambda p: p.value) if points else None

            lift = ((after_avg - before_avg) / before_avg * 100) if before_avg > 0 else float("inf")

            result[kw] = {
                "before_avg": round(before_avg, 2),
                "after_avg": round(after_avg, 2),
                "peak_value": peak.value if peak else 0,
                "peak_date": peak.timestamp.strftime("%Y-%m-%d") if peak else None,
                "lift_pct": round(lift, 1),
                "trend_data": points,
            }

        return result
