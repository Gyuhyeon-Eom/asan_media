"""
아산시 전체 데이터 → PostgreSQL 적재

사용법:
    python load_all_data.py --config ../config/db_config.yaml [--skip-card] [--skip-tmap] [--skip-kcb] [--skip-skt]

데이터 경로는 스크립트 내 DATA_DIRS에서 설정
"""

import argparse
import csv
import io
import os
import sys
import glob
import zipfile

import psycopg2
import psycopg2.extras
import yaml
import pandas as pd

# ============================================================
# 경로 설정
# ============================================================
BASE_DIR = r"C:\Users\HP\IdeaProjects\sundo\asan"
DATA_DIRS = {
    "카드매출": os.path.join(BASE_DIR, "02. 카드매출 데이터", "02. 카드매출 데이터"),
    "신용정보": os.path.join(BASE_DIR, "03. 신용정보 데이터", "03. 신용정보 데이터"),
    "내비게이션": os.path.join(BASE_DIR, "04. 내비게이션 데이터", "04. 내비게이션 데이터"),
    "인구": os.path.join(BASE_DIR, "01. 인구 데이터", "01. 인구 데이터"),
}

def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)["database"]

def get_conn(cfg):
    return psycopg2.connect(host=cfg["host"], port=cfg["port"],
                            dbname=cfg["dbname"], user=cfg["user"], password=cfg["password"])

def safe_int(v):
    if v is None or v == '' or pd.isna(v): return None
    try: return int(float(v))
    except: return None

def safe_float(v):
    if v is None or v == '' or pd.isna(v): return None
    try: return float(v)
    except: return None

# ============================================================
# 1. 카드매출 일별 (ASAN_CSTMR / ASAN_MER)
# ============================================================
def load_card_daily(conn):
    cur = conn.cursor()
    base = DATA_DIRS["카드매출"]
    files = sorted(glob.glob(os.path.join(base, "**", "ASAN_*_DATA_*.csv"), recursive=True))
    print(f"\n[카드 일별] {len(files)}개 파일")

    for fp in files:
        fname = os.path.basename(fp)
        dtype = "CSTMR" if "CSTMR" in fname else "MER"
        print(f"  {fname}...", end=" ", flush=True)

        try:
            df = pd.read_csv(fp, encoding="utf-8-sig", dtype=str)
        except UnicodeDecodeError:
            df = pd.read_csv(fp, encoding="cp949", dtype=str)
        rows = []
        for _, r in df.iterrows():
            rows.append((
                r.get("SALE_DATE",""), r.get("MEGA_CTY_NO",""), r.get("MEGA_CTY_NM",""),
                r.get("CTY_RGN_NO",""), r.get("CTY_RGN_NM",""), r.get("ADMI_CTY_NO",""),
                r.get("ADMI_CTY_NM",""), r.get("MAIN_BUZ_CODE",""), r.get("MAIN_BUZ_DESC",""),
                r.get("TP_GRP_NO",""), r.get("TP_GRP_NM",""), r.get("TP_BUZ_NO",""),
                r.get("TP_BUZ_NM",""), r.get("SEX",""), safe_int(r.get("AGE")),
                safe_int(r.get("SALE_AMT")), safe_int(r.get("SALE_CNT")), dtype
            ))

        if rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO asan_media.card_daily
                (sale_date,mega_cty_no,mega_cty_nm,cty_rgn_no,cty_rgn_nm,admi_cty_no,
                 admi_cty_nm,main_buz_code,main_buz_desc,tp_grp_no,tp_grp_nm,tp_buz_no,
                 tp_buz_nm,sex,age,sale_amt,sale_cnt,data_type)
                VALUES %s
            """, rows, page_size=5000)
            conn.commit()
            print(f"{len(rows)}행")
        else:
            print("빈 파일")

# ============================================================
# 2. 카드매출 월간/주간 소비
# ============================================================
def load_card_consumption(conn):
    cur = conn.cursor()
    base = DATA_DIRS["카드매출"]

    patterns = {
        "CUST_DONG": "AS_{period}_CCND_CUST_CRTR_CCND_CSPT_DONG_{ym}.csv",
        "CUST_DONG_FRCS": "AS_{period}_CCND_CUST_CRTR_CCND_CSPT_DONG_FRCS_{ym}.csv",
        "CUST_SGG": "AS_{period}_CCND_CUST_CRTR_CCND_CSPT_SGG_{ym}.csv",
        "CUST_SGG_FRCS": "AS_{period}_CCND_CUST_CRTR_CCND_CSPT_SGG_FRCS_{ym}.csv",
        "EXCL_LC_DONG": "AS_{period}_CCND_CUST_CRTR_CCND_EXCL_LC_CSPT_DONG_{ym}.csv",
        "EXCL_LC_SGG": "AS_{period}_CCND_CUST_CRTR_CCND_EXCL_LC_CSPT_SGG_{ym}.csv",
        "EXCL_RSDT_DONG": "AS_{period}_CCND_EXCL_RSDT_CRTR_CCND_CSPT_DONG_{ym}.csv",
        "EXCL_RSDT_SGG": "AS_{period}_CCND_EXCL_RSDT_CRTR_CCND_CSPT_SGG_{ym}.csv",
    }

    files = sorted(glob.glob(os.path.join(base, "**", "AS_*CCND*.csv"), recursive=True))
    print(f"\n[카드 소비] {len(files)}개 파일")

    for fp in files:
        fname = os.path.basename(fp)
        if "ASAN_" in fname:
            continue

        # 카테고리/기간 판별
        period_type = "WEEK" if "_WEEK_" in fname else "MM"
        cat = "UNKNOWN"
        for c in patterns:
            key = c.replace("_", "")
            if key.lower() in fname.lower().replace("_", ""):
                cat = c
                break
        # 더 정확한 판별
        if "EXCL_LC" in fname: cat = "EXCL_LC_DONG" if "DONG" in fname else "EXCL_LC_SGG"
        elif "EXCL_RSDT" in fname: cat = "EXCL_RSDT_DONG" if "DONG" in fname else "EXCL_RSDT_SGG"
        elif "FRCS" in fname: cat = "CUST_DONG_FRCS" if "DONG" in fname else "CUST_SGG_FRCS"
        elif "DONG" in fname: cat = "CUST_DONG"
        elif "SGG" in fname: cat = "CUST_SGG"

        print(f"  {fname} [{period_type}/{cat}]...", end=" ", flush=True)

        try:
            df = pd.read_csv(fp, encoding="utf-8-sig", dtype=str)
        except:
            try:
                df = pd.read_csv(fp, encoding="cp949", dtype=str)
            except:
                print("읽기 실패"); continue

        rows = []
        for _, r in df.iterrows():
            crtr = r.get("CRTR_YM") or r.get("CRTR_WEEK", "")
            rows.append((
                str(crtr).strip(), period_type, cat,
                r.get("CUST_CTPV_NM",""), r.get("CUST_SGG_NM",""),
                r.get("CUST_DONG_NM",""), r.get("CUST_DONG_CD") or r.get("CUST_SGG_CD",""),
                r.get("FRCS_CTPV_NM",""), r.get("FRCS_SGG_NM",""),
                r.get("FRCS_DONG_NM",""), r.get("FRCS_DONG_CD") or r.get("FRCS_SGG_CD",""),
                r.get("TOBIZ_CD",""), r.get("TOBIZ_NM",""), r.get("FRCS_SLS_SZ",""),
                safe_int(r.get("ALL_USE_NOCS")), safe_int(r.get("INDIV_USE_NOCS")),
                safe_int(r.get("CORP_USE_NOCS")), safe_int(r.get("ML_USE_NOCS")),
                safe_int(r.get("FM_USE_NOCS")), safe_int(r.get("TEEN_BELOW_USE_NOCS")),
                safe_int(r.get("TEEN_USE_NOCS")), safe_int(r.get("TWT_USE_NOCS")),
                safe_int(r.get("TRT_USE_NOCS")), safe_int(r.get("FRT_USE_NOCS")),
                safe_int(r.get("FFT_USE_NOCS")), safe_int(r.get("SXT_USE_NOCS")),
                safe_int(r.get("SVT_ABV_USE_NOCS")),
                safe_int(r.get("ALL_USE_AMT")), safe_int(r.get("INDIV_USE_AMT")),
                safe_int(r.get("CORP_USE_AMT")), safe_int(r.get("ML_USE_AMT")),
                safe_int(r.get("FM_USE_AMT")), safe_int(r.get("TEEN_BELOW_USE_AMT")),
                safe_int(r.get("TEEN_USE_AMT")), safe_int(r.get("TWT_USE_AMT")),
                safe_int(r.get("TRT_USE_AMT")), safe_int(r.get("FRT_USE_AMT")),
                safe_int(r.get("FFT_USE_AMT")), safe_int(r.get("SXT_USE_AMT")),
                safe_int(r.get("SVT_ABV_USE_AMT")), safe_int(r.get("FRCS_CNT"))
            ))

        if rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO asan_media.card_consumption
                (crtr_period,period_type,data_category,
                 cust_ctpv_nm,cust_sgg_nm,cust_dong_nm,cust_area_cd,
                 frcs_ctpv_nm,frcs_sgg_nm,frcs_dong_nm,frcs_area_cd,
                 tobiz_cd,tobiz_nm,frcs_sls_sz,
                 all_use_nocs,indiv_use_nocs,corp_use_nocs,ml_use_nocs,fm_use_nocs,
                 teen_below_nocs,teen_nocs,twt_nocs,trt_nocs,frt_nocs,fft_nocs,sxt_nocs,svt_abv_nocs,
                 all_use_amt,indiv_use_amt,corp_use_amt,ml_use_amt,fm_use_amt,
                 teen_below_amt,teen_amt,twt_amt,trt_amt,frt_amt,fft_amt,sxt_amt,svt_abv_amt,
                 frcs_cnt)
                VALUES %s
            """, rows, page_size=5000)
            conn.commit()
            print(f"{len(rows)}행")
        else:
            print("빈 파일")

# ============================================================
# 3. T맵 내비게이션
# ============================================================
def load_tmap(conn):
    cur = conn.cursor()
    base = DATA_DIRS["내비게이션"]
    files = sorted(glob.glob(os.path.join(base, "**", "*.csv"), recursive=True))
    print(f"\n[T맵] {len(files)}개 파일")

    for fp in files:
        fname = os.path.basename(fp)
        print(f"  {fname}...", end=" ", flush=True)

        try:
            df = pd.read_csv(fp, encoding="utf-8-sig", dtype=str)
        except UnicodeDecodeError:
            df = pd.read_csv(fp, encoding="cp949", dtype=str)
        rows = []
        for _, r in df.iterrows():
            rows.append((
                r.get("drv_ymd",""), r.get("frst_dptre_ctpv_nm",""), r.get("frst_dptre_sgg_nm",""),
                r.get("dstn_nm",""), safe_float(r.get("dstn_coord_x")), safe_float(r.get("dstn_coord_y")),
                r.get("dstn_ctpv_nm",""), r.get("dstn_sgg_nm",""), r.get("dstn_dong_nm",""),
                r.get("dstn_addr",""), r.get("dstn_ctgy",""),
                safe_int(r.get("vst_cnt")), safe_int(r.get("ntv_vst_cnt")),
                safe_int(r.get("fm_user_cnt")), safe_int(r.get("ml_user_cnt")),
                safe_int(r.get("sx_abs_user_cnt")),
                safe_int(r.get("twt_les_user_cnt")), safe_int(r.get("trt_user_cnt")),
                safe_int(r.get("frt_user_cnt")), safe_int(r.get("fft_user_cnt")),
                safe_int(r.get("sxt_abv_user_cnt")), safe_int(r.get("age_abs_user_cnt")),
                safe_float(r.get("avg_drv_min")), safe_float(r.get("avg_drv_dstc")),
                safe_float(r.get("avg_stay_min")),
                safe_int(r.get("frst_arvl_hr")), safe_int(r.get("scnd_arvl_hr")),
                safe_int(r.get("thrd_arvl_hr")),
                r.get("frst_next_dstn_nm",""), r.get("scnd_next_dstn_nm",""), r.get("thrd_next_dstn_nm","")
            ))

        if rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO asan_media.tmap_od
                (drv_ymd,frst_dptre_ctpv_nm,frst_dptre_sgg_nm,dstn_nm,dstn_coord_x,dstn_coord_y,
                 dstn_ctpv_nm,dstn_sgg_nm,dstn_dong_nm,dstn_addr,dstn_ctgy,
                 vst_cnt,ntv_vst_cnt,fm_user_cnt,ml_user_cnt,sx_abs_user_cnt,
                 twt_les_cnt,trt_cnt,frt_cnt,fft_cnt,sxt_abv_cnt,age_abs_cnt,
                 avg_drv_min,avg_drv_dstc,avg_stay_min,
                 frst_arvl_hr,scnd_arvl_hr,thrd_arvl_hr,
                 frst_next_dstn,scnd_next_dstn,thrd_next_dstn)
                VALUES %s
            """, rows, page_size=5000)
            conn.commit()
            print(f"{len(rows)}행")
        else:
            print("빈 파일")

# ============================================================
# 4. KCB 신용정보 (ZIP 안 CSV 또는 직접 CSV)
# ============================================================
def load_kcb(conn):
    cur = conn.cursor()
    base = DATA_DIRS["신용정보"]
    # CSV 직접 탐색
    files = sorted(glob.glob(os.path.join(base, "**", "AS_KCB_CREDTINFO_*.csv"), recursive=True))
    files += sorted(glob.glob(os.path.join(base, "**", "AS_KCB_CREDTINFO3_*.csv"), recursive=True))
    # ZIP 안 CSV
    zips = sorted(glob.glob(os.path.join(base, "**", "*.zip"), recursive=True))

    print(f"\n[KCB] CSV: {len(files)}개, ZIP: {len(zips)}개")

    all_files = []
    for fp in files:
        all_files.append(("file", fp, os.path.basename(fp)))
    for zp in zips:
        with zipfile.ZipFile(zp) as zf:
            for n in zf.namelist():
                if "CREDTINFO" in n and n.endswith(".csv") and ".pia" not in n:
                    all_files.append(("zip", zp, n))

    for src_type, path, name in all_files:
        dtype = "INFO3" if "INFO3" in name else "INFO"
        print(f"  {name} [{dtype}]...", end=" ", flush=True)

        try:
            if src_type == "zip":
                with zipfile.ZipFile(path) as zf:
                    with zf.open(name) as f:
                        try:
                            df = pd.read_csv(f, encoding="utf-8-sig", dtype=str, nrows=None)
                        except UnicodeDecodeError:
                            df = pd.read_csv(f, encoding="cp949", dtype=str, nrows=None)
            else:
                try:
                    df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
                except UnicodeDecodeError:
                    df = pd.read_csv(path, encoding="cp949", dtype=str)
        except:
            print("읽기 실패"); continue

        rows = []
        for _, r in df.iterrows():
            dong = r.get("DONG_CD") or r.get("STDG_CD", "")
            rows.append((
                r.get("CRTR_YM",""), str(dong).strip(),
                r.get("SX",""), safe_int(r.get("AGE_DVS")),
                safe_int(r.get("NOPE")), safe_int(r.get("ML_NOPE")), safe_int(r.get("FM_NOPE")),
                safe_int(r.get("SLR_NOPE")), safe_int(r.get("S_OWNR_NOPE")),
                safe_int(r.get("FRGNR_NOPE")), safe_int(r.get("AVG_DSTC")),
                safe_int(r.get("MM_INCM")), safe_int(r.get("MID_INCM")),
                safe_int(r.get("HOUSE_OWRN_NOPE")), safe_int(r.get("CAR_OWNR_NOPE")),
                safe_int(r.get("CARD_OWRN_NOPE")), safe_int(r.get("LON_OWNR_NOPE")),
                safe_int(r.get("LON_AVG_BLC")), safe_int(r.get("CR_AVG")),
                safe_int(r.get("ECNM_ACTV_PPLTN_CNT")), dtype
            ))

        if rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO asan_media.kcb_credit
                (crtr_ym,dong_cd,sx,age_dvs,nope,ml_nope,fm_nope,
                 slr_nope,s_ownr_nope,frgnr_nope,avg_dstc,
                 mm_incm,mid_incm,house_owrn_nope,car_ownr_nope,
                 card_owrn_nope,lon_ownr_nope,lon_avg_blc,cr_avg,ecnm_actv_cnt,data_type)
                VALUES %s
            """, rows, page_size=5000)
            conn.commit()
            print(f"{len(rows)}행")
        else:
            print("빈 파일")

# ============================================================
# 5. SKT 소용량 (시군구/읍면동 단위만)
# ============================================================
def load_skt_small(conn):
    cur = conn.cursor()
    base = DATA_DIRS["인구"]

    # AGE_UNQ_OUTFLOW
    files = sorted(glob.glob(os.path.join(base, "**", "AS_SKT_AGE_UNQ_OUTFLOW_NOPE_*.csv"), recursive=True))
    print(f"\n[SKT 유출인구] {len(files)}개")
    for fp in files:
        fname = os.path.basename(fp)
        print(f"  {fname}...", end=" ", flush=True)
        try:
            df = pd.read_csv(fp, encoding="utf-8-sig")
        except UnicodeDecodeError:
            df = pd.read_csv(fp, encoding="cp949")
        rows = []
        for _, r in df.iterrows():
            rows.append(tuple([
                str(r.get("CRTR_YM","")), str(r.get("CRTR_YMD","")),
                str(r.get("SGG_CD","")), str(r.get("OUTFLOW_SGG_CD",""))
            ] + [safe_float(r.iloc[i]) if i < len(r) else None for i in range(4, 32)]))

        if rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO asan_media.skt_age_outflow
                (crtr_ym,crtr_ymd,sgg_cd,outflow_sgg_cd,
                 ml_10_below,ml_10_14,ml_15_19,ml_20_24,ml_25_29,ml_30_34,ml_35_39,
                 ml_40_44,ml_45_49,ml_50_54,ml_55_59,ml_60_64,ml_65_69,ml_70_abv,
                 fm_10_below,fm_10_14,fm_15_19,fm_20_24,fm_25_29,fm_30_34,fm_35_39,
                 fm_40_44,fm_45_49,fm_50_54,fm_55_59,fm_60_64,fm_65_69,fm_70_abv)
                VALUES %s
            """, rows, page_size=5000)
            conn.commit()
            print(f"{len(rows)}행")

    # DOW_UNIQUE
    files = sorted(glob.glob(os.path.join(base, "**", "AS_SKT_DOW_UNIQUE_NOPE_YM_*.csv"), recursive=True))
    print(f"\n[SKT 요일별 유니크] {len(files)}개")
    for fp in files:
        fname = os.path.basename(fp)
        print(f"  {fname}...", end=" ", flush=True)
        try:
            df = pd.read_csv(fp, encoding="utf-8-sig")
        except UnicodeDecodeError:
            df = pd.read_csv(fp, encoding="cp949")
        rows = []
        for _, r in df.iterrows():
            rows.append((
                str(r.get("CRTR_YM","")), str(r.get("SGG_CD","")), str(r.get("INFLOW_SGG_CD","")),
                safe_float(r.get("MON_PPLTN_CNT")), safe_float(r.get("TUES_PPLTN_CNT")),
                safe_float(r.get("WEDNES_PPLTN_CNT")), safe_float(r.get("THURS_PPLTN_CNT")),
                safe_float(r.get("FRI_PPLTN_CNT")), safe_float(r.get("SATUR_PPLTN_CNT")),
                safe_float(r.get("SUN_PPLTN_CNT"))
            ))
        if rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO asan_media.skt_dow_unique
                (crtr_ym,sgg_cd,inflow_sgg_cd,mon_cnt,tues_cnt,wednes_cnt,thurs_cnt,fri_cnt,satur_cnt,sun_cnt)
                VALUES %s
            """, rows, page_size=5000)
            conn.commit()
            print(f"{len(rows)}행")

# ============================================================
# Main
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="아산시 전체 데이터 → PG 적재")
    parser.add_argument("--config", required=True)
    parser.add_argument("--skip-card", action="store_true")
    parser.add_argument("--skip-tmap", action="store_true")
    parser.add_argument("--skip-kcb", action="store_true")
    parser.add_argument("--skip-skt", action="store_true")
    parser.add_argument("--schema-only", action="store_true")
    args = parser.parse_args()

    cfg = load_config(args.config)
    conn = get_conn(cfg)
    print(f"DB: {cfg['host']}:{cfg['port']}/{cfg['dbname']}")

    # 스키마 생성
    schema_path = os.path.join(os.path.dirname(__file__), "..", "sql", "schema_v2.sql")
    if os.path.exists(schema_path):
        with open(schema_path, "r", encoding="utf-8") as f:
            conn.cursor().execute(f.read())
        conn.commit()
        print("스키마 V2 생성 완료")

    if args.schema_only:
        conn.close(); return

    if not args.skip_card:
        load_card_daily(conn)
        load_card_consumption(conn)
    if not args.skip_tmap:
        load_tmap(conn)
    if not args.skip_kcb:
        load_kcb(conn)
    if not args.skip_skt:
        load_skt_small(conn)

    conn.close()
    print("\n전체 적재 완료!")

if __name__ == "__main__":
    main()
