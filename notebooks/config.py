"""
공통 설정 - 경로, 방송 이벤트, 유틸리티
"""
from pathlib import Path
import pandas as pd

# ============================================================
# 경로 설정 (회사 PC 기준)
# ============================================================
BASE_DATA = Path(r"C:\Users\admin\Desktop\작업 폴더\01. work\02. 프로젝트\아산시\방송\01.데이터")
BROADCAST_DIR = BASE_DATA / "방송계획"
CARD_DIR = BASE_DATA / "02. 카드매출 데이터"
TMAP_DIR = BASE_DATA / "04. 내비게이션 데이터"
POP_DIR = BASE_DATA / "01. 인구 데이터"
KCB_DIR = BASE_DATA / "03. 신용정보 데이터"

# ============================================================
# 외지인 소비 정의 (★ 핵심 설계 결정)
# ============================================================
# 주(主) 지표: EXCL_RSDT (거주자 제외) - 아산시 주소 보유자의 소비를 제외
# 강건성 검증: EXCL_LC (현지 제외) - 두 정의에서 결과 일치 시 "외지인 정의에 robust"
#
# ★ 패널 지역 키: 반드시 FRCS_DONG (가맹점 위치) 사용
#   - CUST_DONG(고객 거주지)으로 잡으면 "온천동 사는 사람이 어디서 썼나"가 돼서 분석이 틀어짐
#   - 방송에 나온 건 "장소(가맹점이 있는 동네)"이므로 FRCS_DONG이 정석

# 파일 패턴 매핑
CARD_FILE_PATTERNS = {
    "main": "AS_WEEK_CCND_EXCL_RSDT_CRTR_CCND_CSPT_DONG",   # 주 지표: 거주자 제외, 주간, 동 단위
    "robust": "AS_WEEK_CCND_CUST_CRTR_CCND_EXCL_LC_CSPT_DONG",  # 강건성: 현지 제외, 주간, 동 단위
    "monthly_detail": "AS_MM_CCND_CUST_CRTR_CCND_CSPT_DONG",  # 월간 상세 (업종/성별/연령)
}

# ============================================================
# 관광/소비 관련 업종 키워드 (BC카드 TOBIZ_NM 기준)
# ============================================================
TOURISM_KEYWORDS = {
    "숙박": ["호텔", "모텔", "펜션", "민박", "게스트하우스", "리조트", "콘도", "숙박"],
    "음식": ["한식", "중식", "일식", "양식", "분식", "카페", "제과", "패스트푸드", "치킨",
             "음식", "식당", "요리"],
    "관광레저": ["여행사", "관광", "놀이공원", "온천", "스파", "레저", "캠핑", "유원지"],
    "쇼핑": ["기념품", "특산물", "전통시장", "재래시장"],
    "교통": ["주유소", "주차장", "렌터카", "택시"],
}
# 전체 관광 키워드 flatten
ALL_TOURISM_KEYWORDS = [kw for kwlist in TOURISM_KEYWORDS.values() for kw in kwlist]

# ============================================================
# 방송별 촬영 읍면동 매핑 (broadcast_locations.csv 기반)
# ============================================================
BROADCAST_DONG_MAP = {
    "전국노래자랑 아산시편": ["온천동"],
    "전현무계획2": None,  # "복수" → 특정 불가, 아산시 전체
    "굿모닝 대한민국": ["온천동", "염치읍", "신창면"],
    "6시 내고향": None,  # 상세 촬영지 미공개
    "박원숙의 같이삽시다 시즌3": ["염치읍", "온천동", "영인면", "송악면"],
    "뛰어야산다2": ["온천동", "염치읍"],
    "황제성의 황제파워": ["온천동"],
}

# 방송에 한 번이라도 나온 읍면동 → 처치군
ALL_TREATED_DONGS = {"온천동", "염치읍", "신창면", "영인면", "송악면"}
# 통제군 = 전체 읍면동 - ALL_TREATED_DONGS (데이터 로드 후 확정)

# 방송 그룹핑 (분석 구간)
BROADCAST_GROUPS = {
    "A_전국노래자랑": {
        "air_date": "2025-06-08",
        "pre": ("2025-01", "2025-05"),
        "post": ("2025-06", "2025-07"),
        "dongs": ["온천동"],
        "isolated": True,
    },
    "B_11월캠페인": {
        "air_date": "2025-11-07",  # 첫 방송일
        "air_end": "2025-12-15",   # 마지막 방송 종료
        "pre": ("2025-07", "2025-10"),
        "post": ("2025-11", "2025-12"),
        "dongs": ["온천동", "염치읍", "신창면", "영인면", "송악면"],
        "isolated": False,
        "note": "전현무+굿모닝+6시내고향+같이삽시다 묶음",
    },
    "C_뛰어야산다2": {
        "air_date": "2026-01-12",
        "pre": ("2025-10", "2025-12"),  # 주의: 11월 캠페인 잔류 가능
        "post": ("2026-01", "2026-02"),
        "dongs": ["온천동", "염치읍"],
        "isolated": True,
        "note": "같이삽시다(12/15)→뛰어야산다(1/12) 28일 간격, DID 교차검증 필수",
    },
}


# ============================================================
# 유틸리티 함수
# ============================================================
def read_csv_auto(path, nrows=None):
    """인코딩 자동 감지 CSV 읽기"""
    for enc in ['utf-8', 'utf-8-sig', 'cp949', 'euc-kr']:
        try:
            return pd.read_csv(path, encoding=enc, nrows=nrows)
        except (UnicodeDecodeError, UnicodeError):
            continue
    return pd.read_csv(path, encoding='latin1', nrows=nrows)


def load_monthly_data(folder, file_pattern, months=None):
    """월별 폴더에서 특정 패턴 파일을 합쳐서 로드

    Args:
        folder: 데이터 폴더 (e.g. CARD_DIR)
        file_pattern: 파일명 패턴 (e.g. "AS_WEEK_CCND_EXCL_RSDT_CRTR_CCND_CSPT_DONG")
        months: 로드할 월 목록 (e.g. ["202501", "202502"]), None이면 전체
    """
    dfs = []
    for sub in sorted(folder.iterdir()):
        if not sub.is_dir():
            continue
        ym = sub.name
        if months and ym not in months:
            continue
        for f in sub.iterdir():
            if f.is_file() and file_pattern in f.stem and f.suffix == '.csv':
                df = read_csv_auto(f)
                dfs.append(df)
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


def classify_tourism(tobiz_nm):
    """업종명 → 관광업종 분류"""
    nm = str(tobiz_nm)
    for category, keywords in TOURISM_KEYWORDS.items():
        if any(kw in nm for kw in keywords):
            return category
    return "기타"
