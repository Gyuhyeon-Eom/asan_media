-- ============================================================
-- 아산시 빅데이터 분석 - 추가 스키마 V2
-- 카드매출 + KCB 신용정보 + T맵 내비 + SKT 유동인구 + 현대카드
-- ============================================================

-- 기존 asan_media 스키마 사용
CREATE SCHEMA IF NOT EXISTS asan_media;

-- ============================================================
-- 1. 카드매출 - 고객/가맹점 기준 일별
-- ============================================================
CREATE TABLE IF NOT EXISTS asan_media.card_daily (
    id              BIGSERIAL PRIMARY KEY,
    sale_date       VARCHAR(15)    NOT NULL,
    mega_cty_no     VARCHAR(10),
    mega_cty_nm     VARCHAR(20),
    cty_rgn_no      VARCHAR(10),
    cty_rgn_nm      VARCHAR(20),
    admi_cty_no     VARCHAR(15),
    admi_cty_nm     VARCHAR(20),
    main_buz_code   VARCHAR(10),
    main_buz_desc   VARCHAR(50),
    tp_grp_no       VARCHAR(10),
    tp_grp_nm       VARCHAR(50),
    tp_buz_no       VARCHAR(10),
    tp_buz_nm       VARCHAR(50),
    sex             VARCHAR(5),
    age             INTEGER,
    sale_amt        BIGINT,
    sale_cnt        INTEGER,
    data_type       VARCHAR(10) NOT NULL  -- 'CSTMR' or 'MER'
);
CREATE INDEX IF NOT EXISTS idx_card_daily_date ON asan_media.card_daily(sale_date);
CREATE INDEX IF NOT EXISTS idx_card_daily_dong ON asan_media.card_daily(admi_cty_no);

-- ============================================================
-- 2. 카드매출 - 월간/주간 소비 (공통 구조)
-- ============================================================
CREATE TABLE IF NOT EXISTS asan_media.card_consumption (
    id              BIGSERIAL PRIMARY KEY,
    crtr_period     VARCHAR(15)    NOT NULL,   -- YYYYMM or YYYYWW
    period_type     VARCHAR(10)    NOT NULL,   -- 'MM' or 'WEEK'
    data_category   VARCHAR(30)   NOT NULL,   -- 'CUST_DONG','CUST_SGG','EXCL_LC_DONG','EXCL_RSDT_DONG' 등
    cust_ctpv_nm    VARCHAR(20),
    cust_sgg_nm     VARCHAR(20),
    cust_dong_nm    VARCHAR(20),
    cust_area_cd    VARCHAR(15),
    frcs_ctpv_nm    VARCHAR(20),
    frcs_sgg_nm     VARCHAR(20),
    frcs_dong_nm    VARCHAR(20),
    frcs_area_cd    VARCHAR(15),
    tobiz_cd        VARCHAR(10),
    tobiz_nm        VARCHAR(30),
    frcs_sls_sz     VARCHAR(5),
    all_use_nocs    INTEGER,
    indiv_use_nocs  INTEGER,
    corp_use_nocs   INTEGER,
    ml_use_nocs     INTEGER,
    fm_use_nocs     INTEGER,
    teen_below_nocs INTEGER,
    teen_nocs       INTEGER,
    twt_nocs        INTEGER,
    trt_nocs        INTEGER,
    frt_nocs        INTEGER,
    fft_nocs        INTEGER,
    sxt_nocs        INTEGER,
    svt_abv_nocs    INTEGER,
    all_use_amt     BIGINT,
    indiv_use_amt   BIGINT,
    corp_use_amt    BIGINT,
    ml_use_amt      BIGINT,
    fm_use_amt      BIGINT,
    teen_below_amt  BIGINT,
    teen_amt        BIGINT,
    twt_amt         BIGINT,
    trt_amt         BIGINT,
    frt_amt         BIGINT,
    fft_amt         BIGINT,
    sxt_amt         BIGINT,
    svt_abv_amt     BIGINT,
    frcs_cnt        INTEGER
);
CREATE INDEX IF NOT EXISTS idx_card_cons_period ON asan_media.card_consumption(crtr_period, period_type);
CREATE INDEX IF NOT EXISTS idx_card_cons_cat ON asan_media.card_consumption(data_category);

-- ============================================================
-- 3. 카드매출 - 업종별 매출규모 상세
-- ============================================================
CREATE TABLE IF NOT EXISTS asan_media.card_biz_sales (
    id              BIGSERIAL PRIMARY KEY,
    crtr_period     VARCHAR(15)    NOT NULL,
    period_type     VARCHAR(10)    NOT NULL,
    dong_cd         VARCHAR(15),
    dong_nm         VARCHAR(20),
    tobiz_cd        VARCHAR(10),
    tobiz_nm        VARCHAR(30),
    frcs_sls_sz     VARCHAR(5),
    use_nocs_sum    BIGINT,
    use_amt_sum     BIGINT,
    min_use_nocs    INTEGER,
    mid_use_nocs    NUMERIC,
    max_use_nocs    INTEGER,
    min_use_amt     BIGINT,
    mid_use_amt     BIGINT,
    max_use_amt     BIGINT,
    indiv_use_nocs  BIGINT,
    corp_use_nocs   BIGINT,
    indiv_use_amt   BIGINT,
    corp_use_amt    BIGINT,
    cnnd_cust_nocs  BIGINT,
    cnnd_excl_nocs  BIGINT,
    cnnd_cust_amt   BIGINT,
    cnnd_excl_amt   BIGINT,
    ntv_use_nocs    BIGINT,
    frgnr_use_nocs  BIGINT,
    ntv_use_amt     BIGINT,
    frgnr_use_amt   BIGINT
);

-- ============================================================
-- 4. 카드매출 - 개폐업 현황
-- ============================================================
CREATE TABLE IF NOT EXISTS asan_media.card_biz_open_close (
    id              BIGSERIAL PRIMARY KEY,
    crtr_ym         VARCHAR(6)    NOT NULL,
    dong_cd         VARCHAR(15),
    dong_nm         VARCHAR(20),
    tobiz_cd        VARCHAR(10),
    tobiz_nm        VARCHAR(30),
    frcs_sls_sz     VARCHAR(5),
    biz_mm_cnt      INTEGER,
    frcs_cnt        INTEGER,
    fran_frcs_cnt   INTEGER,
    new_frcs_cnt    INTEGER,
    tcbiz_frcs_cnt  INTEGER,
    clsbiz_frcs_cnt INTEGER,
    yr1_below_cnt   INTEGER,
    yr1_2_cnt       INTEGER,
    yr2_3_cnt       INTEGER,
    yr3_4_cnt       INTEGER,
    yr4_5_cnt       INTEGER,
    yr5_abv_cnt     INTEGER
);

-- ============================================================
-- 5. KCB 신용정보
-- ============================================================
CREATE TABLE IF NOT EXISTS asan_media.kcb_credit (
    id              BIGSERIAL PRIMARY KEY,
    crtr_ym         VARCHAR(6)    NOT NULL,
    dong_cd         VARCHAR(15)    NOT NULL,
    sx              VARCHAR(5),
    age_dvs         INTEGER,
    nope            INTEGER,
    ml_nope         INTEGER,
    fm_nope         INTEGER,
    slr_nope        INTEGER,
    s_ownr_nope     INTEGER,
    frgnr_nope      INTEGER,
    avg_dstc        INTEGER,
    mm_incm         INTEGER,
    mid_incm        INTEGER,
    house_owrn_nope INTEGER,
    car_ownr_nope   INTEGER,
    card_owrn_nope  INTEGER,
    lon_ownr_nope   INTEGER,
    lon_avg_blc     BIGINT,
    cr_avg          INTEGER,
    ecnm_actv_cnt   INTEGER,
    data_type       VARCHAR(10)   -- 'INFO' or 'INFO3'
);
CREATE INDEX IF NOT EXISTS idx_kcb_ym ON asan_media.kcb_credit(crtr_ym);
CREATE INDEX IF NOT EXISTS idx_kcb_dong ON asan_media.kcb_credit(dong_cd);

-- ============================================================
-- 6. T맵 내비게이션 OD
-- ============================================================
CREATE TABLE IF NOT EXISTS asan_media.tmap_od (
    id              BIGSERIAL PRIMARY KEY,
    drv_ymd         VARCHAR(15)    NOT NULL,
    frst_dptre_ctpv_nm VARCHAR(20),
    frst_dptre_sgg_nm  VARCHAR(30),
    dstn_nm         VARCHAR(100),
    dstn_coord_x    NUMERIC(12,6),
    dstn_coord_y    NUMERIC(12,6),
    dstn_ctpv_nm    VARCHAR(20),
    dstn_sgg_nm     VARCHAR(20),
    dstn_dong_nm    VARCHAR(20),
    dstn_addr       VARCHAR(200),
    dstn_ctgy       VARCHAR(100),
    vst_cnt         INTEGER,
    ntv_vst_cnt     INTEGER,
    fm_user_cnt     INTEGER,
    ml_user_cnt     INTEGER,
    sx_abs_user_cnt INTEGER,
    twt_les_cnt     INTEGER,
    trt_cnt         INTEGER,
    frt_cnt         INTEGER,
    fft_cnt         INTEGER,
    sxt_abv_cnt     INTEGER,
    age_abs_cnt     INTEGER,
    avg_drv_min     NUMERIC(10,2),
    avg_drv_dstc    NUMERIC(10,2),
    avg_stay_min    NUMERIC(10,2),
    frst_arvl_hr    INTEGER,
    scnd_arvl_hr    INTEGER,
    thrd_arvl_hr    INTEGER,
    frst_next_dstn  VARCHAR(100),
    scnd_next_dstn  VARCHAR(100),
    thrd_next_dstn  VARCHAR(100)
);
CREATE INDEX IF NOT EXISTS idx_tmap_date ON asan_media.tmap_od(drv_ymd);
CREATE INDEX IF NOT EXISTS idx_tmap_dstn ON asan_media.tmap_od(dstn_dong_nm);

-- ============================================================
-- 7. SKT 유동인구 - 읍면동/시군구 단위 (소용량)
-- ============================================================
CREATE TABLE IF NOT EXISTS asan_media.skt_age_outflow (
    id              BIGSERIAL PRIMARY KEY,
    crtr_ym         VARCHAR(6),
    crtr_ymd        VARCHAR(15),
    sgg_cd          VARCHAR(5),
    outflow_sgg_cd  VARCHAR(5),
    ml_10_below     NUMERIC, ml_10_14 NUMERIC, ml_15_19 NUMERIC,
    ml_20_24 NUMERIC, ml_25_29 NUMERIC, ml_30_34 NUMERIC,
    ml_35_39 NUMERIC, ml_40_44 NUMERIC, ml_45_49 NUMERIC,
    ml_50_54 NUMERIC, ml_55_59 NUMERIC, ml_60_64 NUMERIC,
    ml_65_69 NUMERIC, ml_70_abv NUMERIC,
    fm_10_below NUMERIC, fm_10_14 NUMERIC, fm_15_19 NUMERIC,
    fm_20_24 NUMERIC, fm_25_29 NUMERIC, fm_30_34 NUMERIC,
    fm_35_39 NUMERIC, fm_40_44 NUMERIC, fm_45_49 NUMERIC,
    fm_50_54 NUMERIC, fm_55_59 NUMERIC, fm_60_64 NUMERIC,
    fm_65_69 NUMERIC, fm_70_abv NUMERIC
);

CREATE TABLE IF NOT EXISTS asan_media.skt_dow_unique (
    id              BIGSERIAL PRIMARY KEY,
    crtr_ym         VARCHAR(6),
    sgg_cd          VARCHAR(5),
    inflow_sgg_cd   VARCHAR(5),
    mon_cnt NUMERIC, tues_cnt NUMERIC, wednes_cnt NUMERIC,
    thurs_cnt NUMERIC, fri_cnt NUMERIC, satur_cnt NUMERIC, sun_cnt NUMERIC
);

CREATE TABLE IF NOT EXISTS asan_media.skt_sx_age_dong (
    id              BIGSERIAL PRIMARY KEY,
    crtr_ym         VARCHAR(6),
    crtr_ymd        VARCHAR(15),
    dong_cd         VARCHAR(10),
    dong_nm         VARCHAR(20),
    -- 성별x연령 인구 컬럼들 (동적으로 처리)
    data_json       JSONB
);
CREATE INDEX IF NOT EXISTS idx_skt_dong_date ON asan_media.skt_sx_age_dong(crtr_ymd);

-- ============================================================
-- 8. 현대카드 매출
-- ============================================================
CREATE TABLE IF NOT EXISTS asan_media.hyundae_card (
    id              BIGSERIAL PRIMARY KEY,
    crtr_ym         VARCHAR(6),
    data_type       VARCHAR(20),   -- 'DOW_SLS', 'SX_AGE_SLS', 'TIME_SLS'
    data_json       JSONB
);
CREATE INDEX IF NOT EXISTS idx_hyundae_ym ON asan_media.hyundae_card(crtr_ym);
