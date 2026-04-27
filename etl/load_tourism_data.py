"""
아산시 관광데이터랩 ZIP → PostgreSQL 적재 스크립트

사용법:
    python load_tourism_data.py --data-dir /path/to/데이터 --config config/db_config.local.yaml

데이터 폴더 구조 (관광데이터랩 다운로드):
    데이터/
    ├── 지역별 방문자수/
    │   ├── 내국인 (KT)/     ← ZIP 파일들 (2018~2026)
    │   └── 외국인 (SKT)/    ← ZIP 파일들 (2020~2026)
    ├── 지역별 관광지출액/
    │   ├── 내국인/외지인/   ← ZIP 파일들
    │   ├── 내국인/현지인/   ← ZIP 파일들
    │   └── 외국인/          ← ZIP 파일들
    └── 공공데이터 (방송).txt
"""

import argparse
import csv
import io
import os
import re
import sys
import tempfile
import zipfile
from pathlib import Path

import psycopg2
import psycopg2.extras
import yaml


# ============================================================
# 유틸
# ============================================================

def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["database"]


def get_connection(cfg: dict):
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
    )


def extract_year_range(zip_name: str) -> str:
    """ZIP 파일명에서 시작 연도 추출. 예: '아산시_201801-201812' → '2018'"""
    m = re.search(r"_(\d{4})\d{2}-\d{6}_", zip_name)
    if m:
        return m.group(1)
    # 단년도 fallback
    m = re.search(r"_(\d{4})\d{2}-(\d{4})\d{2}_", zip_name)
    if m:
        return m.group(1)
    return "unknown"


def read_csv_from_zip(zip_path: str) -> dict[str, list[dict]]:
    """ZIP 안의 모든 CSV를 dict로 반환. key=파일명(확장자 제외), value=rows"""
    result = {}
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".csv"):
                continue
            raw = zf.read(name)
            # 인코딩 탐색
            for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
                try:
                    text = raw.decode(enc)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
            else:
                print(f"  [WARN] 인코딩 실패: {name}")
                continue

            reader = csv.DictReader(io.StringIO(text))
            rows = list(reader)

            # 파일명에서 타임스탬프 접두사 제거하고 핵심 이름 추출
            base = os.path.splitext(os.path.basename(name))[0]
            # '20260410161850_방문자 수 추이' → '방문자 수 추이'
            cleaned = re.sub(r"^\d+_", "", base)
            result[cleaned] = rows
    return result


def safe_int(val) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def safe_float(val) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ============================================================
# 적재 함수들
# ============================================================

def load_visitor_domestic(conn, data_dir: str):
    """내국인 방문자수 (KT) 적재"""
    folder = os.path.join(data_dir, "지역별 방문자수", "내국인 (KT)")
    if not os.path.isdir(folder):
        print(f"[SKIP] 폴더 없음: {folder}")
        return

    cur = conn.cursor()
    zips = sorted([f for f in os.listdir(folder) if f.endswith(".zip")])
    print(f"\n[내국인 방문자수] {len(zips)}개 ZIP 처리")

    for zf_name in zips:
        zf_path = os.path.join(folder, zf_name)
        year = extract_year_range(zf_name)
        csvs = read_csv_from_zip(zf_path)
        print(f"  {zf_name} → year={year}, CSVs={list(csvs.keys())}")

        # 방문자 수 추이
        if "방문자 수 추이" in csvs:
            rows = csvs["방문자 수 추이"]
            for r in rows:
                cur.execute("""
                    INSERT INTO asan_media.visitor_domestic_trend
                        (base_ym, municipality, visitor_type, visitor_count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (base_ym, municipality, visitor_type) DO UPDATE
                        SET visitor_count = EXCLUDED.visitor_count
                """, (
                    r.get("기준년월", "").strip(),
                    r.get("기초지자체", "").strip(),
                    r.get("방문자 구분", "").strip(),
                    safe_int(r.get("방문자 수")),
                ))

        # 지역별 방문자 수
        if "지역별 방문자 수" in csvs:
            rows = csvs["지역별 방문자 수"]
            for r in rows:
                cur.execute("""
                    INSERT INTO asan_media.visitor_domestic_by_region
                        (base_year, region_name, visitor_count, visitor_ratio)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (base_year, region_name) DO UPDATE
                        SET visitor_count = EXCLUDED.visitor_count,
                            visitor_ratio = EXCLUDED.visitor_ratio
                """, (
                    year,
                    r.get("기초지자체명", "").strip(),
                    safe_int(r.get("기초지자체 방문자 수")),
                    safe_float(r.get("기초지자체 방문자 비율")),
                ))

        # 방문자 거주지
        if "방문자 거주지" in csvs:
            rows = csvs["방문자 거주지"]
            for r in rows:
                cur.execute("""
                    INSERT INTO asan_media.visitor_domestic_origin
                        (base_year, origin_sido, origin_sigungu, ratio_pct)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (base_year, origin_sido, origin_sigungu) DO UPDATE
                        SET ratio_pct = EXCLUDED.ratio_pct
                """, (
                    year,
                    r.get("거주지(시도)", "").strip(),
                    r.get("거주지(시군구)", "").strip(),
                    safe_float(r.get("비율(%)")),
                ))

    conn.commit()
    print(f"  [OK] 내국인 방문자수 적재 완료")


def load_visitor_foreign(conn, data_dir: str):
    """외국인 방문자수 (SKT) 적재"""
    folder = os.path.join(data_dir, "지역별 방문자수", "외국인 (SKT)")
    if not os.path.isdir(folder):
        print(f"[SKIP] 폴더 없음: {folder}")
        return

    cur = conn.cursor()
    zips = sorted([f for f in os.listdir(folder) if f.endswith(".zip")])
    print(f"\n[외국인 방문자수] {len(zips)}개 ZIP 처리")

    for zf_name in zips:
        zf_path = os.path.join(folder, zf_name)
        year = extract_year_range(zf_name)
        csvs = read_csv_from_zip(zf_path)
        print(f"  {zf_name} → year={year}, CSVs={list(csvs.keys())}")

        # 외국인 방문자 수 추이
        if "외국인 방문자 수 추이" in csvs:
            rows = csvs["외국인 방문자 수 추이"]
            for r in rows:
                cur.execute("""
                    INSERT INTO asan_media.visitor_foreign_trend
                        (base_ym, municipality, visitor_count)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (base_ym, municipality) DO UPDATE
                        SET visitor_count = EXCLUDED.visitor_count
                """, (
                    r.get("날짜", "").strip(),
                    r.get("지역", "").strip(),
                    safe_int(r.get("외국인 방문자수")),
                ))

        # 외국인 지역별 방문자 수
        if "외국인 지역별 방문자 수" in csvs:
            rows = csvs["외국인 지역별 방문자 수"]
            for r in rows:
                cur.execute("""
                    INSERT INTO asan_media.visitor_foreign_by_region
                        (base_year, region_name, visitor_count)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (base_year, region_name) DO UPDATE
                        SET visitor_count = EXCLUDED.visitor_count
                """, (
                    year,
                    r.get("지역", "").strip(),
                    safe_int(r.get("외국인 방문자수")),
                ))

        # 외국인 거주지(국가)
        if "외국인 방문자 거주지(국가)" in csvs:
            rows = csvs["외국인 방문자 거주지(국가)"]
            for r in rows:
                cur.execute("""
                    INSERT INTO asan_media.visitor_foreign_origin
                        (base_year, country_name, ratio_pct)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (base_year, country_name) DO UPDATE
                        SET ratio_pct = EXCLUDED.ratio_pct
                """, (
                    year,
                    r.get("국가명", "").strip(),
                    safe_float(r.get("비율(%)")),
                ))

    conn.commit()
    print(f"  [OK] 외국인 방문자수 적재 완료")


def load_spending(conn, data_dir: str):
    """관광지출액 적재 (내국인 외지인/현지인, 외국인)"""
    categories = [
        ("지역별 관광지출액/내국인/외지인", "outsider"),
        ("지역별 관광지출액/내국인/현지인", "local"),
        ("지역별 관광지출액/외국인", "foreign"),
    ]

    cur = conn.cursor()

    for rel_path, cat_code in categories:
        folder = os.path.join(data_dir, *rel_path.split("/"))
        if not os.path.isdir(folder):
            print(f"[SKIP] 폴더 없음: {folder}")
            continue

        zips = sorted([f for f in os.listdir(folder) if f.endswith(".zip")])
        print(f"\n[관광지출액/{cat_code}] {len(zips)}개 ZIP 처리")

        for zf_name in zips:
            zf_path = os.path.join(folder, zf_name)
            year = extract_year_range(zf_name)
            csvs = read_csv_from_zip(zf_path)
            print(f"  {zf_name} → year={year}, CSVs={list(csvs.keys())}")

            # 관광소비 추이
            if "관광소비 추이" in csvs:
                rows = csvs["관광소비 추이"]
                for r in rows:
                    prev = r.get("전년도 지출액")
                    cur.execute("""
                        INSERT INTO asan_media.spending_trend
                            (base_ym, municipality, visitor_category, subcategory,
                             spending_krw_1000, prev_year_spending)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (base_ym, municipality, visitor_category, subcategory) DO UPDATE
                            SET spending_krw_1000 = EXCLUDED.spending_krw_1000,
                                prev_year_spending = EXCLUDED.prev_year_spending
                    """, (
                        r.get("기준년월", "").strip(),
                        r.get("기초지자체", "").strip(),
                        cat_code,
                        r.get("중분류", "").strip(),
                        safe_int(r.get("소비액(천원)")),
                        safe_int(prev) if prev else None,
                    ))

            # 업종별 지출액
            if "업종별 지출액" in csvs:
                rows = csvs["업종별 지출액"]
                for r in rows:
                    cur.execute("""
                        INSERT INTO asan_media.spending_by_industry
                            (base_year, visitor_category, major_category, sub_category,
                             major_ratio_pct, sub_ratio_pct)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (base_year, visitor_category, major_category, sub_category) DO UPDATE
                            SET major_ratio_pct = EXCLUDED.major_ratio_pct,
                                sub_ratio_pct = EXCLUDED.sub_ratio_pct
                    """, (
                        year,
                        cat_code,
                        r.get("대분류", "").strip(),
                        r.get("중분류", "").strip(),
                        safe_float(r.get("대분류 지출액 비율")),
                        safe_float(r.get("중분류 지출액 비율")),
                    ))

            # 지역별 지출액
            key_region = "지역별 지출액"
            if key_region in csvs:
                rows = csvs[key_region]
                for r in rows:
                    # 컬럼명이 카테고리마다 다름: '행정동명' or '지역명'
                    region = (r.get("행정동명") or r.get("지역명") or "").strip()
                    cur.execute("""
                        INSERT INTO asan_media.spending_by_region
                            (base_year, visitor_category, region_name, ratio_pct)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (base_year, visitor_category, region_name) DO UPDATE
                            SET ratio_pct = EXCLUDED.ratio_pct
                    """, (
                        year,
                        cat_code,
                        region,
                        safe_float(r.get("비율(%)")),
                    ))

    conn.commit()
    print(f"  [OK] 관광지출액 적재 완료")


# ============================================================
# 메인
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="아산시 관광데이터랩 → PostgreSQL 적재")
    parser.add_argument("--data-dir", required=True, help="데이터 폴더 경로")
    parser.add_argument("--config", required=True, help="DB 설정 YAML 경로")
    parser.add_argument("--schema-only", action="store_true", help="스키마만 생성 (데이터 적재 안 함)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    conn = get_connection(cfg)
    print(f"DB 연결: {cfg['host']}:{cfg['port']}/{cfg['dbname']}")

    # 스키마 생성
    schema_sql = os.path.join(os.path.dirname(__file__), "..", "sql", "schema.sql")
    if os.path.exists(schema_sql):
        with open(schema_sql, "r", encoding="utf-8") as f:
            conn.cursor().execute(f.read())
        conn.commit()
        print("스키마 생성 완료")

    if args.schema_only:
        print("--schema-only: 스키마만 생성하고 종료")
        conn.close()
        return

    # 데이터 적재
    load_visitor_domestic(conn, args.data_dir)
    load_visitor_foreign(conn, args.data_dir)
    load_spending(conn, args.data_dir)

    conn.close()
    print("\n전체 적재 완료!")


if __name__ == "__main__":
    main()
