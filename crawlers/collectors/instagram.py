"""Instagram 해시태그 기반 게시물 수집기.

⚠️ 비공식 웹 스크래핑 — rate limit 주의, 구조 변경 시 수정 필요.
   개인 연구/분석 용도로만 사용.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup

from models import CollectionResult, ContentItem, Platform
from .base import BaseCollector


class InstagramCollector(BaseCollector):
    """Instagram 해시태그 기반 게시물 수집.

    로그인 없이 공개 해시태그 페이지에서 수집.
    최근 게시물 ~70개 정도만 접근 가능.
    """

    platform = Platform.INSTAGRAM
    rate_limit_per_sec = 0.2  # 5초에 1회

    def __init__(self):
        super().__init__()
        self.client.headers.update({
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "ko-KR,ko;q=0.9",
        })

    def collect(
        self,
        keyword: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_items: int = 50,
    ) -> CollectionResult:
        tag = keyword.strip().lstrip("#").replace(" ", "")
        start_dt, end_dt = self.date_range_params(start_date, end_date, default_days_back=30)

        try:
            data = self._fetch_tag_page(tag)
            posts = self._extract_posts(data)
        except Exception as e:
            self.logger.error(f"Instagram 수집 실패: {e}")
            return CollectionResult(
                platform=Platform.INSTAGRAM, keyword=keyword, error=str(e),
            )

        items: list[ContentItem] = []
        for post in posts[:max_items]:
            pub_dt = self._ts_to_dt(post.get("taken_at_timestamp"))
            if pub_dt and (pub_dt < start_dt or pub_dt > end_dt):
                continue

            shortcode = post.get("shortcode", "")
            likes = post.get("edge_liked_by", post.get("edge_media_preview_like", {}))
            comments = post.get("edge_media_to_comment", post.get("edge_media_preview_comment", {}))

            ci = ContentItem(
                platform=Platform.INSTAGRAM,
                keyword=keyword,
                title=self._caption(post)[:200],
                url=f"https://www.instagram.com/p/{shortcode}/" if shortcode else "",
                author=post.get("owner", {}).get("username", ""),
                published_at=pub_dt,
                like_count=likes.get("count", 0),
                comment_count=comments.get("count", 0),
                view_count=post.get("video_view_count", 0),
                raw=post,
            )
            items.append(ci)

        self.logger.info(f"[instagram] '#{tag}' → {len(items)}건")
        return CollectionResult(
            platform=Platform.INSTAGRAM,
            keyword=keyword,
            items=items,
            total_count=len(items),
        )

    def _fetch_tag_page(self, tag: str) -> dict:
        resp = self.get(f"https://www.instagram.com/explore/tags/{tag}/")
        for extractor in [self._shared_data, self._additional_data]:
            data = extractor(resp.text)
            if data:
                return data
        raise ValueError("Instagram 페이지 데이터 추출 실패")

    def _extract_posts(self, data: dict) -> list[dict]:
        paths = [
            lambda d: d["entry_data"]["TagPage"][0]["graphql"]["hashtag"]["edge_hashtag_to_media"]["edges"],
            lambda d: d["graphql"]["hashtag"]["edge_hashtag_to_media"]["edges"],
            lambda d: d["data"]["hashtag"]["edge_hashtag_to_media"]["edges"],
        ]
        for fn in paths:
            try:
                edges = fn(data)
                if edges:
                    return [e.get("node", e) for e in edges]
            except (KeyError, IndexError, TypeError):
                continue
        return []

    @staticmethod
    def _shared_data(html: str) -> Optional[dict]:
        m = re.search(r"window\._sharedData\s*=\s*(\{.+?\});</script>", html, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                pass
        return None

    @staticmethod
    def _additional_data(html: str) -> Optional[dict]:
        m = re.search(r"window\.__additionalDataLoaded\([^,]+,\s*(\{.+?\})\);", html, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                pass
        return None

    @staticmethod
    def _caption(post: dict) -> str:
        edges = post.get("edge_media_to_caption", {}).get("edges", [])
        return edges[0].get("node", {}).get("text", "") if edges else ""

    @staticmethod
    def _ts_to_dt(ts) -> Optional[datetime]:
        try:
            return datetime.fromtimestamp(int(ts)) if ts else None
        except (ValueError, OSError):
            return None
