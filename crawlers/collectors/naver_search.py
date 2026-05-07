"""네이버 검색 API 수집기 — 블로그/뉴스/카페.

아산 관광 콘텐츠 수집에 특화:
- 블로그: 여행 후기, 맛집 리뷰 등 UGC
- 뉴스: 방송 보도, 관광 기사
- 카페: 커뮤니티 반응, 여행 정보 공유
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Optional

from models import CollectionResult, ContentItem, Platform
from .base import BaseCollector

_ENDPOINTS = {
    Platform.NAVER_BLOG: "https://openapi.naver.com/v1/search/blog.json",
    Platform.NAVER_NEWS: "https://openapi.naver.com/v1/search/news.json",
    Platform.NAVER_CAFE: "https://openapi.naver.com/v1/search/cafearticle.json",
}


class NaverSearchCollector(BaseCollector):
    """네이버 블로그/뉴스/카페 검색 수집기.

    API: https://developers.naver.com/docs/serviceapi/search/blog/blog.md
    제한: 일 25,000건, 1회 최대 100건, 페이징 최대 start=1000
    """

    platform = Platform.NAVER_BLOG
    rate_limit_per_sec = 10.0

    def __init__(self):
        super().__init__()
        self.client_id = os.getenv("NAVER_CLIENT_ID", "")
        self.client_secret = os.getenv("NAVER_CLIENT_SECRET", "")
        if not self.client_id:
            self.logger.warning("NAVER_CLIENT_ID 미설정")

    def _headers(self) -> dict:
        return {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
        }

    def collect(
        self,
        keyword: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_items: int = 100,
        search_types: Optional[list[Platform]] = None,
    ) -> CollectionResult:
        """네이버 블로그+뉴스+카페 통합 수집."""

        if search_types is None:
            search_types = [Platform.NAVER_BLOG, Platform.NAVER_NEWS, Platform.NAVER_CAFE]

        start_dt, end_dt = self.date_range_params(start_date, end_date)
        all_items: list[ContentItem] = []
        grand_total = 0

        for stype in search_types:
            endpoint = _ENDPOINTS[stype]
            collected = 0
            start_idx = 1

            while collected < max_items and start_idx <= 1000:
                batch = min(100, max_items - collected)
                params = {
                    "query": keyword,
                    "display": batch,
                    "start": start_idx,
                    "sort": "date",
                }
                resp = self.get(endpoint, params=params, headers=self._headers())
                data = resp.json()

                items = data.get("items", [])
                if not items:
                    break

                grand_total = max(grand_total, data.get("total", 0))

                for item in items:
                    pub_dt = self._parse_date(
                        item.get("postdate", item.get("pubDate", ""))
                    )
                    if pub_dt:
                        if pub_dt < start_dt or pub_dt > end_dt:
                            continue

                    ci = ContentItem(
                        platform=stype,
                        keyword=keyword,
                        title=self._strip_html(item.get("title", "")),
                        url=item.get("link", item.get("originallink", "")),
                        description=self._strip_html(item.get("description", "")),
                        author=item.get("bloggername", item.get("cafename", "")),
                        published_at=pub_dt,
                        raw=item,
                    )
                    all_items.append(ci)
                    collected += 1

                start_idx += batch

            self.logger.info(f"[{stype.value}] '{keyword}' → {collected}건")

        return CollectionResult(
            platform=Platform.NAVER_BLOG,
            keyword=keyword,
            items=all_items,
            total_count=grand_total,
        )

    @staticmethod
    def _strip_html(text: str) -> str:
        return re.sub(r"<[^>]+>", "", text).strip()

    @staticmethod
    def _parse_date(date_str: str) -> Optional[datetime]:
        if not date_str:
            return None
        if len(date_str) == 8 and date_str.isdigit():
            try:
                return datetime.strptime(date_str, "%Y%m%d")
            except ValueError:
                return None
        try:
            return parsedate_to_datetime(date_str)
        except Exception:
            return None
