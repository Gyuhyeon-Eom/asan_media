"""수집기 베이스 + 관광 콘텐츠 자동 분류기."""

from __future__ import annotations

import abc
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Optional

import httpx
from fake_useragent import UserAgent
from tenacity import (
    retry, stop_after_attempt, wait_exponential, retry_if_exception,
)


def _should_retry(exception):
    """5xx, 연결 에러만 재시도. 4xx(400 Bad Request 등)는 즉시 실패."""
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code >= 500
    if isinstance(exception, httpx.ConnectError):
        return True
    return False

from config import (
    TourismCategory, CLASSIFICATION_PATTERNS, ATTRACTION_KEYWORDS,
    FOOD_KEYWORDS, ACCOMMODATION_KEYWORDS,
)
from models import CollectionResult, ContentItem, Platform

logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self, calls_per_second: float = 1.0):
        self.min_interval = 1.0 / calls_per_second
        self._last_call = 0.0

    def wait(self):
        now = time.monotonic()
        elapsed = now - self._last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_call = time.monotonic()


class TourismClassifier:
    """콘텐츠의 제목+본문을 보고 관광 카테고리 자동 분류."""

    # 아산 관광지/맛집 명칭 → 언급 추출용
    SPOT_NAMES = (
        ATTRACTION_KEYWORDS + FOOD_KEYWORDS + ACCOMMODATION_KEYWORDS
        + ["온양온천", "도고온천", "아산 스파비스", "외암마을", "현충사",
           "공세리성당", "피나클랜드", "신정호", "지중해마을"]
    )

    @classmethod
    def classify(cls, title: str, description: str = "") -> TourismCategory:
        """제목 + 본문 텍스트로 카테고리 판별."""
        text = f"{title} {description}".lower()

        scores: dict[TourismCategory, int] = {}
        for cat, patterns in CLASSIFICATION_PATTERNS.items():
            score = sum(1 for p in patterns if p in text)
            if score > 0:
                scores[cat] = score

        if not scores:
            return TourismCategory.GENERAL

        return max(scores, key=scores.get)

    @classmethod
    def extract_spots(cls, title: str, description: str = "") -> list[str]:
        """텍스트에서 언급된 아산 관광지/맛집 명칭 추출."""
        text = f"{title} {description}"
        found = []
        for spot in cls.SPOT_NAMES:
            # 공백 제거 버전도 매칭 (예: "외암 민속마을" → "외암민속마을")
            spot_normalized = spot.replace(" ", "")
            if spot in text or spot_normalized in text.replace(" ", ""):
                found.append(spot)
        return list(dict.fromkeys(found))  # 중복 제거, 순서 유지

    @classmethod
    def is_review(cls, title: str, description: str = "") -> bool:
        """실제 방문 후기인지 판별."""
        review_signals = [
            "후기", "리뷰", "다녀왔", "다녀옴", "방문기", "여행기",
            "솔직후기", "갔다왔", "먹어봤", "방문후기", "체험후기",
        ]
        text = f"{title} {description}".lower()
        return any(sig in text for sig in review_signals)

    @classmethod
    def enrich_item(cls, item: ContentItem) -> ContentItem:
        """ContentItem에 관광 분류 정보 부착."""
        item.tourism_category = cls.classify(item.title, item.description)
        item.mentioned_spots = cls.extract_spots(item.title, item.description)
        item.is_review = cls.is_review(item.title, item.description)
        return item


class BaseCollector(abc.ABC):
    """모든 플랫폼 수집기의 베이스."""

    platform: Platform
    rate_limit_per_sec: float = 1.0

    def __init__(self):
        self.rate_limiter = RateLimiter(self.rate_limit_per_sec)
        self.ua = UserAgent()
        self.client = httpx.Client(
            timeout=30.0,
            headers={"User-Agent": self.ua.random},
            follow_redirects=True,
        )
        self.classifier = TourismClassifier()
        self.logger = logging.getLogger(self.__class__.__name__)

    def __del__(self):
        try:
            self.client.close()
        except Exception:
            pass

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception(_should_retry),
    )
    def _request(self, method: str, url: str, **kwargs) -> httpx.Response:
        self.rate_limiter.wait()
        self.client.headers["User-Agent"] = self.ua.random
        resp = self.client.request(method, url, **kwargs)
        resp.raise_for_status()
        return resp

    def get(self, url: str, **kwargs) -> httpx.Response:
        return self._request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> httpx.Response:
        return self._request("POST", url, **kwargs)

    @abc.abstractmethod
    def collect(
        self,
        keyword: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_items: int = 100,
    ) -> CollectionResult:
        ...

    def safe_collect(self, keyword: str, **kwargs) -> CollectionResult:
        """에러 핸들링 포함 수집. 실패해도 빈 결과 반환."""
        try:
            result = self.collect(keyword, **kwargs)
            # 모든 아이템에 관광 카테고리 자동 분류 적용
            result.items = [self.classifier.enrich_item(item) for item in result.items]
            return result
        except Exception as e:
            self.logger.error(f"[{self.platform.value}] 수집 실패: {e}", exc_info=True)
            return CollectionResult(platform=self.platform, keyword=keyword, error=str(e))

    @staticmethod
    def date_range_params(
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        default_days_back: int = 14,
    ) -> tuple[datetime, datetime]:
        end = end_date or datetime.now()
        start = start_date or (end - timedelta(days=default_days_back))
        return start, end
