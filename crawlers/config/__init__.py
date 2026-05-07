"""아산 관광 홍보효과 분석 — 키워드 분류 체계 및 설정.

아산시청 방송(TV 출연 + 자체 SNS 콘텐츠) 후
관광 관련 크롤링 지표 변화를 측정하기 위한 키워드/카테고리 정의.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class TourismCategory(str, Enum):
    """관광 콘텐츠 분류 카테고리."""
    ATTRACTION = "관광지"        # 관광 명소, 볼거리
    FOOD = "맛집"               # 음식점, 카페
    ACCOMMODATION = "숙박"      # 호텔, 펜션, 한옥 스테이
    EXPERIENCE = "체험"         # 체험 프로그램, 축제
    GENERAL = "일반"            # 위에 해당 안 되는 아산 언급
    HOT_SPRING = "온천"         # 온양온천 특화
    FESTIVAL = "축제"           # 아산 축제/이벤트
    REVIEW = "후기"             # 여행 후기, 방문 리뷰


# ──────────────────────────────────────────────
# 아산 관광 키워드 사전
# ──────────────────────────────────────────────

# 코어 키워드: 방송 전후 검색량 변화를 직접 추적
CORE_KEYWORDS = [
    "아산 여행",
    "아산 관광",
    "아산 가볼만한곳",
    "아산 당일치기",
]

# 관광지별 키워드
ATTRACTION_KEYWORDS = [
    "외암민속마을",
    "아산 현충사",
    "아산 지중해마을",
    "아산 피나클랜드",
    "아산 공세리성당",
    "신정호수공원",
    "아산 세계꽃식물원",
    "온양온천",
    "아산 스파비스",
    "도고온천",
    "아산 레인보우시티",
]

# 맛집/카페 키워드
FOOD_KEYWORDS = [
    "아산 맛집",
    "온양 맛집",
    "아산 카페",
    "아산 한우",
    "아산 순대국",
]

# 숙박 키워드
ACCOMMODATION_KEYWORDS = [
    "아산 숙소",
    "아산 호텔",
    "아산 펜션",
    "온양 호텔",
    "아산 한옥스테이",
]

# 체험/축제 키워드
EXPERIENCE_KEYWORDS = [
    "아산 축제",
    "아산 체험",
    "아산 온천축제",
    "아산 벚꽃",
]

# 카테고리 → 키워드 매핑
CATEGORY_KEYWORD_MAP: dict[TourismCategory, list[str]] = {
    TourismCategory.ATTRACTION: ATTRACTION_KEYWORDS,
    TourismCategory.FOOD: FOOD_KEYWORDS,
    TourismCategory.ACCOMMODATION: ACCOMMODATION_KEYWORDS,
    TourismCategory.EXPERIENCE: EXPERIENCE_KEYWORDS,
    TourismCategory.HOT_SPRING: ["온양온천", "도고온천", "아산 온천", "아산 스파"],
}

# 전체 키워드 (중복 제거)
ALL_KEYWORDS: list[str] = list(dict.fromkeys(
    CORE_KEYWORDS
    + ATTRACTION_KEYWORDS
    + FOOD_KEYWORDS
    + ACCOMMODATION_KEYWORDS
    + EXPERIENCE_KEYWORDS
))

# ──────────────────────────────────────────────
# 콘텐츠 분류용 패턴
# ──────────────────────────────────────────────

# 제목/본문에서 카테고리 자동 분류할 때 사용하는 키워드 패턴
CLASSIFICATION_PATTERNS: dict[TourismCategory, list[str]] = {
    TourismCategory.FOOD: [
        "맛집", "카페", "식당", "먹거리", "메뉴", "맛있", "음식",
        "한우", "순대", "국밥", "칼국수", "브런치", "디저트",
    ],
    TourismCategory.ACCOMMODATION: [
        "숙소", "호텔", "펜션", "모텔", "게스트하우스", "숙박",
        "체크인", "객실", "1박", "2박",
    ],
    TourismCategory.HOT_SPRING: [
        "온천", "스파", "온양", "도고", "노천탕", "족욕",
    ],
    TourismCategory.ATTRACTION: [
        "관광", "명소", "볼거리", "외암", "현충사", "공세리",
        "피나클", "지중해마을", "신정호", "전망대",
    ],
    TourismCategory.EXPERIENCE: [
        "체험", "축제", "행사", "이벤트", "페스티벌", "마켓",
    ],
    TourismCategory.REVIEW: [
        "후기", "리뷰", "다녀왔", "방문기", "여행기", "솔직후기",
        "추천", "코스", "일정",
    ],
    TourismCategory.FESTIVAL: [
        "축제", "페스티벌", "벚꽃", "국화", "온천축제",
    ],
}


@dataclass
class BroadcastInfo:
    """개별 방송 건 정보."""
    title: str                              # 방송 제목/프로그램명
    broadcast_date: str                     # "YYYY-MM-DD"
    broadcast_type: str = "tv"              # "tv" | "youtube" | "sns"
    channel: str = ""                       # 채널/플랫폼명
    description: str = ""                   # 방송 내용 요약
    featured_spots: list[str] = field(default_factory=list)  # 방송에 노출된 관광지/맛집
    custom_keywords: list[str] = field(default_factory=list) # 이 방송에 특화된 추가 키워드


@dataclass
class CrawlConfig:
    """크롤링 실행 설정."""
    days_before: int = 7           # 방송 전 비교 기간
    days_after: int = 14           # 방송 후 추적 기간
    max_items_per_platform: int = 200
    platforms: list[str] = field(default_factory=lambda: [
        "naver_blog", "naver_news", "naver_cafe",
        "naver_datalab", "youtube", "instagram",
    ])
    use_core_keywords: bool = True
    use_category_keywords: bool = True
    target_categories: list[TourismCategory] = field(
        default_factory=lambda: list(TourismCategory)
    )
