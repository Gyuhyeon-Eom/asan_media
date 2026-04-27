-- ============================================================
-- 아산시 방송 홍보 효과 분석 - PostgreSQL Schema
-- ============================================================

CREATE SCHEMA IF NOT EXISTS asan_media;

-- ------------------------------------------------------------
-- 1. 내국인 방문자수 추이 (KT 데이터)
--    원본: "방문자 수 추이.csv"
--    컬럼: 기준년월, 기초지자체, 방문자 구분, 방문자 수
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asan_media.visitor_domestic_trend (
    id              SERIAL PRIMARY KEY,
    base_ym         VARCHAR(6)    NOT NULL,   -- 기준년월 (YYYYMM)
    municipality    VARCHAR(50)   NOT NULL,   -- 기초지자체 (아산시)
    visitor_type    VARCHAR(30)   NOT NULL,   -- 방문자 구분: 외지인방문자(b), 현지인방문자(a), 전체방문자(a+b)
    visitor_count   BIGINT,                   -- 방문자 수
    UNIQUE (base_ym, municipality, visitor_type)
);

COMMENT ON TABLE asan_media.visitor_domestic_trend IS '내국인 방문자수 월별 추이 (KT, 관광데이터랩)';

-- ------------------------------------------------------------
-- 2. 내국인 지역별(읍면동) 방문자수 (KT 데이터)
--    원본: "지역별 방문자 수.csv"
--    컬럼: 기초지자체명, 기초지자체 방문자 수, 기초지자체 방문자 비율
--    주의: 원본에는 기준년도가 파일 단위로만 있음 → 파싱 시 연도 추가
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asan_media.visitor_domestic_by_region (
    id              SERIAL PRIMARY KEY,
    base_year       VARCHAR(4)    NOT NULL,   -- 기준연도 (YYYY), ZIP 파일명에서 추출
    region_name     VARCHAR(50)   NOT NULL,   -- 읍면동명
    visitor_count   BIGINT,                   -- 방문자 수
    visitor_ratio   NUMERIC(5,2),             -- 비율(%)
    UNIQUE (base_year, region_name)
);

COMMENT ON TABLE asan_media.visitor_domestic_by_region IS '내국인 읍면동별 방문자수 (KT, 관광데이터랩)';

-- ------------------------------------------------------------
-- 3. 내국인 방문자 거주지 (KT 데이터)
--    원본: "방문자 거주지.csv"
--    컬럼: 거주지(시도), 거주지(시군구), 비율(%)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asan_media.visitor_domestic_origin (
    id              SERIAL PRIMARY KEY,
    base_year       VARCHAR(4)    NOT NULL,
    origin_sido     VARCHAR(30)   NOT NULL,   -- 거주지 시도
    origin_sigungu  VARCHAR(50)   NOT NULL,   -- 거주지 시군구
    ratio_pct       NUMERIC(5,2),             -- 비율(%)
    UNIQUE (base_year, origin_sido, origin_sigungu)
);

COMMENT ON TABLE asan_media.visitor_domestic_origin IS '내국인 방문자 거주지별 비율 (KT, 관광데이터랩)';

-- ------------------------------------------------------------
-- 4. 외국인 방문자수 추이 (SKT 데이터)
--    원본: "외국인 방문자 수 추이.csv"
--    컬럼: 날짜, 지역, 외국인 방문자수
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asan_media.visitor_foreign_trend (
    id              SERIAL PRIMARY KEY,
    base_ym         VARCHAR(6)    NOT NULL,
    municipality    VARCHAR(50)   NOT NULL,
    visitor_count   BIGINT,
    UNIQUE (base_ym, municipality)
);

COMMENT ON TABLE asan_media.visitor_foreign_trend IS '외국인 방문자수 월별 추이 (SKT, 관광데이터랩)';

-- ------------------------------------------------------------
-- 5. 외국인 지역별(읍면동) 방문자수
--    원본: "외국인 지역별 방문자 수.csv"
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asan_media.visitor_foreign_by_region (
    id              SERIAL PRIMARY KEY,
    base_year       VARCHAR(4)    NOT NULL,
    region_name     VARCHAR(50)   NOT NULL,
    visitor_count   BIGINT,
    UNIQUE (base_year, region_name)
);

COMMENT ON TABLE asan_media.visitor_foreign_by_region IS '외국인 읍면동별 방문자수 (SKT, 관광데이터랩)';

-- ------------------------------------------------------------
-- 6. 외국인 방문자 거주지(국가)
--    원본: "외국인 방문자 거주지(국가).csv"
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asan_media.visitor_foreign_origin (
    id              SERIAL PRIMARY KEY,
    base_year       VARCHAR(4)    NOT NULL,
    country_name    VARCHAR(50)   NOT NULL,
    ratio_pct       NUMERIC(5,2),
    UNIQUE (base_year, country_name)
);

COMMENT ON TABLE asan_media.visitor_foreign_origin IS '외국인 방문자 출신 국가별 비율 (SKT, 관광데이터랩)';

-- ------------------------------------------------------------
-- 7. 관광지출액 추이 (내국인 외지인 / 내국인 현지인 / 외국인)
--    원본: "관광소비 추이.csv"
--    컬럼: 기준년월, 기초지자체, 중분류, 소비액(천원), [전년도 지출액]
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asan_media.spending_trend (
    id                  SERIAL PRIMARY KEY,
    base_ym             VARCHAR(6)    NOT NULL,
    municipality        VARCHAR(50)   NOT NULL,
    visitor_category    VARCHAR(20)   NOT NULL,   -- 'outsider'(외지인), 'local'(현지인), 'foreign'(외국인)
    subcategory         VARCHAR(50)   NOT NULL,   -- 중분류 (관광총소비, 호텔, 기타관광쇼핑 등)
    spending_krw_1000   BIGINT,                   -- 소비액(천원)
    prev_year_spending  BIGINT,                   -- 전년도 지출액 (없는 경우 NULL)
    UNIQUE (base_ym, municipality, visitor_category, subcategory)
);

COMMENT ON TABLE asan_media.spending_trend IS '관광 소비액 월별 추이 (관광데이터랩)';

-- ------------------------------------------------------------
-- 8. 관광지출액 업종별
--    원본: "업종별 지출액.csv"
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asan_media.spending_by_industry (
    id                  SERIAL PRIMARY KEY,
    base_year           VARCHAR(4)    NOT NULL,
    visitor_category    VARCHAR(20)   NOT NULL,
    major_category      VARCHAR(30)   NOT NULL,   -- 대분류 (쇼핑업, 숙박업 등)
    sub_category        VARCHAR(50)   NOT NULL,   -- 중분류 (기타관광쇼핑 등)
    major_ratio_pct     NUMERIC(5,2),             -- 대분류 지출액 비율
    sub_ratio_pct       NUMERIC(5,2),             -- 중분류 지출액 비율
    UNIQUE (base_year, visitor_category, major_category, sub_category)
);

COMMENT ON TABLE asan_media.spending_by_industry IS '업종별 관광지출 비율 (관광데이터랩)';

-- ------------------------------------------------------------
-- 9. 관광지출액 지역별(읍면동)
--    원본: "지역별 지출액.csv"
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asan_media.spending_by_region (
    id                  SERIAL PRIMARY KEY,
    base_year           VARCHAR(4)    NOT NULL,
    visitor_category    VARCHAR(20)   NOT NULL,
    region_name         VARCHAR(50)   NOT NULL,
    ratio_pct           NUMERIC(5,2),
    UNIQUE (base_year, visitor_category, region_name)
);

COMMENT ON TABLE asan_media.spending_by_region IS '읍면동별 관광지출 비율 (관광데이터랩)';

-- ------------------------------------------------------------
-- 인덱스
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_visitor_dom_trend_ym ON asan_media.visitor_domestic_trend(base_ym);
CREATE INDEX IF NOT EXISTS idx_visitor_for_trend_ym ON asan_media.visitor_foreign_trend(base_ym);
CREATE INDEX IF NOT EXISTS idx_spending_trend_ym ON asan_media.spending_trend(base_ym);
CREATE INDEX IF NOT EXISTS idx_spending_trend_cat ON asan_media.spending_trend(visitor_category);
