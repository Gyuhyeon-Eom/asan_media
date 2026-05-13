"""
아산시 방송 홍보 효과 분석 - 설정 파일
=================================
모든 경로, 방송 이벤트, 관광지 매핑 등을 여기서 관리
"""
import os
import platform
from pathlib import Path

# ============================================================
# 데이터 경로 (OS별 자동 감지)
# ============================================================
if platform.system() == "Windows":
    DATA_DIR = Path(r"C:\Users\HP\Desktop\01.데이터")
    OUTPUT_DIR = Path(r"C:\Users\HP\Desktop\02.분석결과")
else:
    _script_dir = Path(__file__).parent
    DATA_DIR = _script_dir / "data"
    OUTPUT_DIR = _script_dir / "output"

CARD_DIR = DATA_DIR / "02. 카드매출 데이터"
KCB_DIR = DATA_DIR / "03. 신용정보 데이터"
TMAP_DIR = DATA_DIR / "04. 내비게이션 데이터"
POP_DIR = DATA_DIR / "01. 인구 데이터"
VISIT_DIR = DATA_DIR / "지역별 방문자수"
SPEND_DIR = DATA_DIR / "지역별 관광지출액"

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# 방송 이벤트 정의
# ============================================================
BROADCASTS = [
    {
        "name": "전국노래자랑 아산시편",
        "broadcaster": "KBS1",
        "genre": "음악예능",
        "filming_date": "2025-04-19",
        "air_date": "2025-06-08",
        "air_end_date": "2025-06-08",
        "episodes": 1,
        "rating": 6.5,
        "target_demo": "중장년",
        "budget_1000won": 3518,
        "locations": ["신정호 잔디광장"],
        "dong_codes": [44200610],  # 온양5동 (신정호 인근)
        "dong_names": ["온양5동"],
        "related_event": "이순신축제+신정호정원 개원",
        "confounders": ["이순신축제(4~5월)"],
    },
    {
        "name": "전현무계획2",
        "broadcaster": "MBN",
        "genre": "여행버라이어티",
        "filming_date": "2025-10-02",
        "air_date": "2025-11-07",
        "air_end_date": "2025-11-07",
        "episodes": 1,
        "rating": 1.5,
        "target_demo": "2030",
        "budget_1000won": 50000,
        "locations": ["아산시 일원(숨은맛집/명소)"],
        "dong_codes": [],  # 특정 동 없음 - 전체 아산시
        "dong_names": [],
        "related_event": None,
        "confounders": [],
    },
    {
        "name": "굿모닝 대한민국",
        "broadcaster": "KBS2",
        "genre": "교양정보",
        "filming_date": "2025-11-08",
        "air_date": "2025-11-12",
        "air_end_date": "2025-11-16",  # 재방송
        "episodes": 1,
        "rating": 0.55,
        "target_demo": "전연령",
        "budget_1000won": 20000,
        "locations": [
            "온양온천역 광장", "족욕체험장", "온양온천 전통시장",
            "곡교천 은행나무길", "이순신 관광체험센터", "현충사", "피나클랜드"
        ],
        "dong_codes": [44200570, 44200250, 44200340],  # 온양1동, 염치읍, 신창면
        "dong_names": ["온양1동", "염치읍", "신창면"],
        "related_event": None,
        "confounders": ["가을단풍시즌(10~11월)"],
    },
    {
        "name": "6시 내고향",
        "broadcaster": "KBS1",
        "genre": "교양정보",
        "filming_date": "2025-11-13",
        "air_date": "2025-11-13",
        "air_end_date": "2025-11-13",
        "episodes": 1,
        "rating": 5.5,
        "target_demo": "중장년",
        "budget_1000won": 110000,
        "locations": ["아산시 주요명소"],
        "dong_codes": [],
        "dong_names": [],
        "related_event": "아산방문의해",
        "confounders": [],
    },
    {
        "name": "박원숙의 같이삽시다 시즌3",
        "broadcaster": "KBS2",
        "genre": "예능",
        "filming_date": "2025-08-01",  # 8~11월 촬영
        "air_date": "2025-11-24",
        "air_end_date": "2025-12-15",
        "episodes": 4,
        "rating": 3.0,
        "target_demo": "중장년",
        "budget_1000won": 133000,  # 아산분 (총 400000 중)
        "locations": [
            "곡교천 은행나무길", "신정호정원", "영인산",
            "외암민속마을", "온양온천시장", "도고파라다이스", "아산스파포레"
        ],
        "dong_codes": [44200250, 44200610, 44200370, 44200310, 44200570, 44200400, 44200350],
        "dong_names": ["염치읍", "온양5동", "영인면", "송악면", "온양1동", "도고면", "음봉면"],
        "related_event": None,
        "confounders": ["가을단풍시즌(10~11월)", "겨울온천시즌(12월)"],
    },
    {
        "name": "뛰어야산다2",
        "broadcaster": "MBN",
        "genre": "스포츠리얼버라이어티",
        "filming_date": "2025-11-16",
        "air_date": "2026-01-12",
        "air_end_date": "2026-01-12",
        "episodes": 1,
        "rating": 1.5,
        "target_demo": "2030",
        "budget_1000won": 45000,
        "locations": [
            "신정호정원", "곡교천 은행나무길", "현충사", "온천", "캠핑장"
        ],
        "dong_codes": [44200610, 44200250, 44200570],
        "dong_names": ["온양5동", "염치읍", "온양1동"],
        "related_event": None,
        "confounders": ["비수기(1월)"],  # 오히려 계절 교란 적음
    },
    {
        "name": "황제성의 황제파워",
        "broadcaster": "SBS파워FM",
        "genre": "라디오공개방송",
        "filming_date": "2026-05-02",
        "air_date": "2026-05-09",
        "air_end_date": "2026-05-09",
        "episodes": 1,
        "rating": None,
        "target_demo": "전연령",
        "budget_1000won": 220000,
        "locations": ["온양온천역 주무대"],
        "dong_codes": [44200570],
        "dong_names": ["온양1동"],
        "related_event": "제65회 아산성웅이순신축제",
        "confounders": ["이순신축제(4~5월)"],
    },
]

# ============================================================
# 아산시 읍면동 코드 매핑
# ============================================================
ASAN_DONG_MAP = {
    44200250: "염치읍",
    44200253: "배방읍",
    44200310: "송악면",
    44200330: "탕정면",
    44200340: "신창면",
    44200350: "음봉면",
    44200360: "둔포면",
    44200370: "영인면",
    44200380: "인주면",
    44200390: "선장면",
    44200400: "도고면",
    44200570: "온양1동",
    44200580: "온양2동",
    44200590: "온양3동",
    44200600: "온양4동",
    44200610: "온양5동",
    44200620: "온양6동",
}

# 관광 관련 업종 코드 (카드매출에서 필터링용)
TOURISM_BIZ_CODES = {
    # 숙박
    1001: "특급호텔", 1003: "2급호텔", 1010: "콘도", 1020: "기타숙박업",
    # 음식
    8001: "일반한식", 8005: "중국음식", 8006: "서양음식", 8010: "일본음식",
    8013: "주점", 8021: "스넥", 8301: "제과점",
    # 여행/레저
    2199: "기타레져업", 2250: "영화관", 2251: "티켓",
    # 소매
    4010: "편의점", 4020: "슈퍼마켓", 4004: "대형할인점",
    # 교통
    3302: "LPG", 3305: "주유소", 3307: "주유소", 3308: "주유소",
    # 온천/힐링
    7120: "사우나",
}

# 관광 핵심 업종 (좁은 범위 - 방문객 proxy)
CORE_TOURISM_BIZ = [1001, 1003, 1010, 1020, 2199, 7120]
# 음식점 (관광 소비)
FOOD_BIZ = [8001, 8005, 8006, 8010, 8013, 8021, 8301]
# 소매 (관광 소비)
RETAIL_BIZ = [4010, 4020, 4004]

# T맵 관광 카테고리
TMAP_TOURISM_CATEGORIES = [
    "여행/레저_관광명소_온천",
    "여행/레저_관광명소_동식물원",
    "여행/레저_숙박_호텔",
    "여행/레저_숙박_펜션",
    "여행/레저_숙박_콘도",
    "여행/레저_관광명소_유적지",
    "여행/레저_관광명소_공원",
    "여행/레저_관광명소_자연경관",
    "여행/레저_관광명소_전시관",
    "여행/레저_관광명소_박물관",
    "여행/레저_관광명소_체험마을",
]

# 분석 윈도우 (일 단위)
WINDOW_PRE = 28   # 방송 전 4주
WINDOW_POST = 28  # 방송 후 4주
WINDOW_LONG = 90  # 장기 효과 3개월

# 비교군 도시 (계절성 통제용)
CONTROL_CITIES = {
    44131: "천안시 동남구",
    44133: "천안시 서북구",
    44210: "서산시",
    44230: "논산시",
}
