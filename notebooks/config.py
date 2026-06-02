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
# 관광/소비 관련 업종 코드 (BC카드 TOBIZ_CD 기준)
# ============================================================
TOURISM_TOBIZ = {
    "숙박": ["호텔", "모텔", "펜션", "민박", "게스트하우스", "리조트", "콘도"],
    "음식": ["한식", "중식", "일식", "양식", "분식", "카페", "제과", "패스트푸드", "치킨"],
    "관광레저": ["여행사", "관광", "놀이공원", "온천", "스파", "레저", "캠핑"],
    "쇼핑": ["기념품", "특산물", "전통시장"],
    "교통": ["주유소", "주차장", "렌터카", "택시"],
}

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

# 방송에 한 번도 안 나온 읍면동 → 통제군 후보
ALL_TREATED_DONGS = {"온천동", "염치읍", "신창면", "영인면", "송악면"}
# 아산시 전체 읍면동 (데이터에서 확인 후 업데이트)
# CONTROL_DONGS = 전체 - ALL_TREATED_DONGS


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
