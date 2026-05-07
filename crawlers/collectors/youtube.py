"""YouTube Data API v3 수집기.

아산 관련 영상(시청 자체 콘텐츠 + 외부 크리에이터 콘텐츠)의
조회수, 좋아요, 댓글 수 수집.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

from models import CollectionResult, ContentItem, Platform
from .base import BaseCollector

_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
_VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"


class YouTubeCollector(BaseCollector):
    """YouTube 영상 검색 + 통계 수집.

    API 유닛: search.list=100, videos.list=1 (일일 10,000 유닛)
    """

    platform = Platform.YOUTUBE
    rate_limit_per_sec = 5.0

    def __init__(self):
        super().__init__()
        self.api_key = os.getenv("YOUTUBE_API_KEY", "")
        if not self.api_key:
            self.logger.warning("YOUTUBE_API_KEY 미설정")

    def collect(
        self,
        keyword: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_items: int = 50,
    ) -> CollectionResult:
        start_dt, end_dt = self.date_range_params(start_date, end_date)
        items: list[ContentItem] = []
        next_page = None

        while len(items) < max_items:
            params = {
                "part": "snippet",
                "q": keyword,
                "type": "video",
                "order": "date",
                "publishedAfter": start_dt.strftime("%Y-%m-%dT00:00:00Z"),
                "publishedBefore": end_dt.strftime("%Y-%m-%dT23:59:59Z"),
                "maxResults": min(50, max_items - len(items)),
                "key": self.api_key,
                "regionCode": "KR",
                "relevanceLanguage": "ko",
            }
            if next_page:
                params["pageToken"] = next_page

            search_data = self.get(_SEARCH_URL, params=params).json()
            search_items = search_data.get("items", [])
            if not search_items:
                break

            video_ids = [
                it["id"]["videoId"]
                for it in search_items
                if "videoId" in it.get("id", {})
            ]
            stats_map = self._fetch_stats(video_ids) if video_ids else {}

            for it in search_items:
                vid = it.get("id", {}).get("videoId", "")
                snippet = it.get("snippet", {})
                stats = stats_map.get(vid, {})

                ci = ContentItem(
                    platform=Platform.YOUTUBE,
                    keyword=keyword,
                    title=snippet.get("title", ""),
                    url=f"https://www.youtube.com/watch?v={vid}",
                    description=snippet.get("description", "")[:500],
                    author=snippet.get("channelTitle", ""),
                    published_at=self._parse_iso(snippet.get("publishedAt", "")),
                    view_count=int(stats.get("viewCount", 0)),
                    like_count=int(stats.get("likeCount", 0)),
                    comment_count=int(stats.get("commentCount", 0)),
                    raw={"snippet": snippet, "statistics": stats},
                )
                items.append(ci)

            next_page = search_data.get("nextPageToken")
            if not next_page:
                break

        self.logger.info(f"[youtube] '{keyword}' → {len(items)}건")
        return CollectionResult(
            platform=Platform.YOUTUBE,
            keyword=keyword,
            items=items,
            total_count=len(items),
        )

    def _fetch_stats(self, video_ids: list[str]) -> dict[str, dict]:
        stats_map = {}
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i + 50]
            params = {
                "part": "statistics",
                "id": ",".join(batch),
                "key": self.api_key,
            }
            resp = self.get(_VIDEOS_URL, params=params).json()
            for item in resp.get("items", []):
                stats_map[item["id"]] = item.get("statistics", {})
        return stats_map

    @staticmethod
    def _parse_iso(s: str) -> Optional[datetime]:
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None
