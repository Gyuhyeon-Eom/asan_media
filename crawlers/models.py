"""데이터 모델 정의."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class Platform(str, Enum):
    NAVER_BLOG = "naver_blog"
    NAVER_NEWS = "naver_news"
    NAVER_CAFE = "naver_cafe"
    NAVER_DATALAB = "naver_datalab"
    YOUTUBE = "youtube"
    INSTAGRAM = "instagram"


@dataclass
class ContentItem:
    """수집된 콘텐츠 단위."""
    platform: Platform
    keyword: str
    title: str = ""
    url: str = ""
    description: str = ""
    author: str = ""
    published_at: Optional[datetime] = None
    view_count: int = 0
    like_count: int = 0
    comment_count: int = 0
    raw: dict = field(default_factory=dict)
    # 관광 분류 (TourismClassifier가 채움)
    tourism_category: Any = None
    mentioned_spots: list[str] = field(default_factory=list)
    is_review: bool = False

    @property
    def engagement(self) -> int:
        return self.view_count + self.like_count * 5 + self.comment_count * 10


@dataclass
class TrendPoint:
    """검색 트렌드 데이터 포인트."""
    platform: Platform
    keyword: str
    timestamp: datetime
    value: float
    metric_name: str = "search_trend_ratio"


@dataclass
class CollectionResult:
    """플랫폼별 수집 결과."""
    platform: Platform
    keyword: str
    items: list[ContentItem] = field(default_factory=list)
    trends: list[TrendPoint] = field(default_factory=list)
    total_count: int = 0
    error: Optional[str] = None

    @property
    def success(self) -> bool:
        return self.error is None

    @property
    def total_engagement(self) -> int:
        return sum(item.engagement for item in self.items)
