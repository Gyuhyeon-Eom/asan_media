# 아산시 빅데이터 분석 - 데이터 명세서

---

## 1. 카드매출 데이터 (아산시 빅데이터 플랫폼)

### 1-1. ASAN_CSTMR_DATA (고객 기준 카드매출)
> 일별 고객 거주지-가맹점 소재지-업종별 카드 매출 (고객 성별/연령 포함)

| 컬럼명 | 설명 | 예시 |
|--------|------|------|
| SALE_DATE | 매출일자 | 20190101 |
| MEGA_CTY_NO | 광역시도 코드 | 44 |
| MEGA_CTY_NM | 광역시도명 | 충청남도 |
| CTY_RGN_NO | 시군구 코드 | 4420 |
| CTY_RGN_NM | 시군구명 | 아산시 |
| ADMI_CTY_NO | 행정동 코드 | 44200610 |
| ADMI_CTY_NM | 행정동명 | 온양5동 |
| MAIN_BUZ_CODE | 대분류 업종코드 | |
| MAIN_BUZ_DESC | 대분류 업종명 | |
| TP_GRP_NO | 업종그룹 코드 | |
| TP_GRP_NM | 업종그룹명 | |
| TP_BUZ_NO | 세부업종 코드 | |
| TP_BUZ_NM | 세부업종명 | |
| SEX | 성별 (1:남, 2:여) | 1 |
| AGE | 연령대 | 30 |
| SALE_AMT | 매출금액 (원) | 150000 |
| SALE_CNT | 매출건수 | 3 |

### 1-2. ASAN_MER_DATA (가맹점 기준 카드매출)
> ASAN_CSTMR_DATA와 동일 구조, 가맹점 기준 집계

---

### 1-3. AS_MM_CCND_CUST_CRTR_CCND_CSPT_DONG (월간 카드소비 - 고객거주지 기준 - 읍면동)
> 월별 고객거주지(읍면동)-가맹점소재지(읍면동)-업종별 카드 이용 건수/금액

| 컬럼명 | 설명 |
|--------|------|
| CRTR_YM | 기준년월 (YYYYMM) |
| CUST_CTPV_NM | 고객 시도명 |
| CUST_SGG_NM | 고객 시군구명 |
| CUST_DONG_NM | 고객 읍면동명 |
| CUST_DONG_CD | 고객 읍면동 코드 |
| FRCS_CTPV_NM | 가맹점 시도명 |
| FRCS_SGG_NM | 가맹점 시군구명 |
| FRCS_DONG_NM | 가맹점 읍면동명 |
| FRCS_DONG_CD | 가맹점 읍면동 코드 |
| TOBIZ_CD | 업종코드 |
| TOBIZ_NM | 업종명 |
| FRCS_SLS_SZ | 가맹점 매출규모 (A/B/C/D) |
| ALL_USE_NOCS | 전체 이용건수 |
| INDIV_USE_NOCS | 개인 이용건수 |
| CORP_USE_NOCS | 법인 이용건수 |
| ML_USE_NOCS | 남성 이용건수 |
| FM_USE_NOCS | 여성 이용건수 |
| TEEN_BELOW_USE_NOCS | 10대 이하 이용건수 |
| TEEN_USE_NOCS | 10대 이용건수 |
| TWT_USE_NOCS | 20대 이용건수 |
| TRT_USE_NOCS | 30대 이용건수 |
| FRT_USE_NOCS | 40대 이용건수 |
| FFT_USE_NOCS | 50대 이용건수 |
| SXT_USE_NOCS | 60대 이용건수 |
| SVT_ABV_USE_NOCS | 70대 이상 이용건수 |
| ALL_USE_AMT | 전체 이용금액 (원) |
| INDIV_USE_AMT | 개인 이용금액 |
| CORP_USE_AMT | 법인 이용금액 |
| ML_USE_AMT | 남성 이용금액 |
| FM_USE_AMT | 여성 이용금액 |
| TEEN_BELOW_USE_AMT | 10대 이하 이용금액 |
| TEEN_USE_AMT | 10대 이용금액 |
| TWT_USE_AMT | 20대 이용금액 |
| TRT_USE_AMT | 30대 이용금액 |
| FRT_USE_AMT | 40대 이용금액 |
| FFT_USE_AMT | 50대 이용금액 |
| SXT_USE_AMT | 60대 이용금액 |
| SVT_ABV_USE_AMT | 70대 이상 이용금액 |
| FRCS_CNT | 가맹점 수 |

### 파생 파일 (동일 컬럼 구조, 집계 기준만 다름)

| 파일명 패턴 | 시간단위 | 고객기준 | 가맹점기준 | 설명 |
|------------|---------|---------|-----------|------|
| AS_MM_CCND_CUST_CRTR_CCND_CSPT_DONG | 월간 | 읍면동 | 읍면동 | 고객거주지 읍면동 기준 소비 |
| AS_MM_CCND_CUST_CRTR_CCND_CSPT_DONG_FRCS | 월간 | 읍면동 | 읍면동 | 위와 동일 (가맹점 상세) |
| AS_MM_CCND_CUST_CRTR_CCND_CSPT_SGG | 월간 | 시군구 | 시군구 | 시군구 단위 집계 |
| AS_MM_CCND_CUST_CRTR_CCND_CSPT_SGG_FRCS | 월간 | 시군구 | 시군구 | 위와 동일 (가맹점 상세) |
| AS_MM_CCND_CUST_CRTR_CCND_EXCL_LC_CSPT_DONG | 월간 | 읍면동 | 읍면동 | 고객거주지 기준 (지역화폐 제외) |
| AS_MM_CCND_CUST_CRTR_CCND_EXCL_LC_CSPT_SGG | 월간 | 시군구 | 시군구 | 시군구 단위 (지역화폐 제외) |
| AS_MM_CCND_EXCL_RSDT_CRTR_CCND_CSPT_DONG | 월간 | 읍면동 | 읍면동 | 비거주자 기준 소비 |
| AS_MM_CCND_EXCL_RSDT_CRTR_CCND_CSPT_SGG | 월간 | 시군구 | 시군구 | 비거주자 시군구 단위 |
| AS_WEEK_* | 주간 | (위와 동일 패턴) | | 주간 단위 집계 (CRTR_WEEK) |

---

### 1-4. AS_MM_DSST_SPRT_AMT_CUST_CSPT (월간 재난지원금 고객 소비)

| 컬럼명 | 설명 |
|--------|------|
| CRTR_YM | 기준년월 |
| CTPV_NM | 시도명 |
| SGG_NM | 시군구명 |
| DONG_NM | 읍면동명 |
| DONG_CD | 읍면동 코드 |
| EMRG_DSST_SPRT_AMT_RCVMT_NOPE | 긴급재난지원금 수령 인원 |
| EMRG_DSST_SPRT_AMT_GIVE_GRAMT | 긴급재난지원금 지급 총액 |
| ML_USE_NOCS ~ SVT_ABV_USE_NOCS | 성별/연령별 이용건수 |
| ML_USE_AMT ~ SVT_ABV_USE_AMT | 성별/연령별 이용금액 |
| ALL_USE_NOCS | 전체 이용건수 |
| ALL_USE_AMT | 전체 이용금액 |
| USE_FRCS_CNT | 이용 가맹점 수 |

### 1-5. AS_MM_DSST_SPRT_AMT_FRCS_CSPT (월간 재난지원금 가맹점 소비)

| 컬럼명 | 설명 |
|--------|------|
| CRTR_YM | 기준년월 |
| CTPV_NM ~ DONG_CD | 지역 정보 |
| TOBIZ_CD | 업종코드 |
| TOBIZ_NM | 업종명 |
| FRCS_SLS_SZ | 가맹점 매출규모 |
| LCCRC_FRCS_YN | 지역화폐 가맹점 여부 |
| EMRG_DSST_SPRT_AMT_FRCS_YN | 재난지원금 가맹점 여부 |
| FRCS_CNT | 가맹점 수 |
| USE_NOCS | 이용건수 |
| USE_AMT | 이용금액 |

### 1-6. AS_MM_LC_CRTR_TCBIZ_CLSBIZ (월간 업종별 개폐업 현황)

| 컬럼명 | 설명 |
|--------|------|
| CRTR_YM | 기준년월 |
| CTPV_NM ~ DONG_CD | 지역 정보 |
| TOBIZ_CD / TOBIZ_NM | 업종 코드/명 |
| FRCS_SLS_SZ | 가맹점 매출규모 |
| BIZ_MM_CNT | 사업개월수 |
| FRCS_CNT | 가맹점 수 |
| FRAN_FRCS_CNT | 프랜차이즈 가맹점 수 |
| NEW_FRCS_CNT | 신규 가맹점 수 |
| TCBIZ_FRCS_CNT | 개업 가맹점 수 |
| CLSBIZ_FRCS_CNT | 폐업 가맹점 수 |
| ONE_YR_BELOW_FRCS_CNT | 1년 미만 가맹점 |
| ONE_YR_ABV_TWO_YR_BELOW_FRCS_CNT | 1~2년 가맹점 |
| TWO_YR_ABV_THREE_YR_BELOW_FRCS_CNT | 2~3년 가맹점 |
| THREE_YR_ABV_FOUR_YR_BELOW_FRCS_CNT | 3~4년 가맹점 |
| FOUR_YR_ABV_FIVE_YR_BELOW_FRCS_CNT | 4~5년 가맹점 |
| FIVE_YR_ABV_FRCS_CNT | 5년 이상 가맹점 |

### 1-7. AS_MM_LC_CRTR_TOBIZ_SLS_SZ (월간 업종별 매출규모 상세)

| 컬럼명 | 설명 |
|--------|------|
| CRTR_YM | 기준년월 |
| CTPV_NM ~ DONG_CD | 지역 정보 |
| TOBIZ_CD / TOBIZ_NM | 업종 코드/명 |
| FRCS_SLS_SZ | 가맹점 매출규모 |
| USE_NOCS_SUM | 이용건수 합계 |
| USE_AMT_SUM | 이용금액 합계 |
| MIN/MID/MAX_USE_NOCS | 이용건수 최소/중위/최대 |
| MIN/MID/MAX_USE_AMT | 이용금액 최소/중위/최대 |
| INDIV_USE_NOCS_SUM | 개인 이용건수 |
| CORP_USE_NOCS_SUM | 법인 이용건수 |
| INDIV_USE_AMT_SUM | 개인 이용금액 |
| COR_USE_AMT_SUM | 법인 이용금액 |
| REG_FRCS_USE_NOCS_SUM | 단골 가맹점 이용건수 |
| REG_FRCS_USE_AMT_SUM | 단골 가맹점 이용금액 |
| MON~SUN_USE_NOCS | 요일별 이용건수 |
| MON~SUN_USE_AMT | 요일별 이용금액 |
| 00_05_HR ~ 21_23_HR_USE_NOCS | 시간대별 이용건수 |
| 00_05_HR ~ 21_23_HR_USE_AMT | 시간대별 이용금액 |
| CNND_CUST_USE_NOCS | 연동(아산시 거주) 고객 이용건수 |
| CNND_EXCL_CUST_USE_NOCS | 비연동(비거주) 고객 이용건수 |
| CNND_CUST_USE_AMT | 연동 고객 이용금액 |
| CNND_EXCL_CUST_USE_AMT | 비연동 고객 이용금액 |
| NTV_USE_NOCS | 내국인 이용건수 |
| FRGNR_USE_NOCS | 외국인 이용건수 |
| NTV_USE_AMT | 내국인 이용금액 |
| FRGNR_USE_AMT | 외국인 이용금액 |

---

## 2. KCB 신용정보 데이터

### 2-1. AS_KCB_CREDTINFO (읍면동-성별-연령별 신용정보)

| 컬럼명 | 설명 |
|--------|------|
| CRTR_YM | 기준년월 |
| DONG_CD | 읍면동 코드 |
| SX | 성별 (1:남, 2:여) |
| AGE_DVS | 연령대 |
| NOPE | 인원수 |
| ML_NOPE / FM_NOPE | 남성/여성 인원 |
| SLR_NOPE | 급여소득자 인원 |
| S_OWNR_NOPE | 자영업자 인원 |
| ETC_NOPE | 기타 인원 |
| FRGNR_NOPE | 외국인 인원 |
| AVG_DSTC | 평균 신용등급 점수 |
| H_INCM_NOPE | 고소득 인원 |
| MM_INCM | 월평균 소득 (만원) |
| MM_INCM_200D ~ MM_INCM_1000U | 월소득 구간별 분포 |
| MID_INCM | 중위 소득 |
| HOUSE_OWRN_NOPE | 주택 소유 인원 |
| OWRNS_HOUSE_EVL_GRAMT_AVG | 소유주택 평가액 평균 |
| CAR_OWNR_NOPE | 자동차 소유 인원 |
| CAR_OALP_AVG | 자동차 가액 평균 |
| CARD_OWRN_NOPE | 카드 소유 인원 |
| CRCD_OWNR_NOPE | 신용카드 소유 인원 |
| CRCD_OWNRS_CNT_AVG | 신용카드 소유 장수 평균 |
| C_CARD_OWRN_NOPE | 체크카드 소유 인원 |
| CARD_GRAMT_AVG | 카드 한도 평균 |
| CR_NTSL_USE_GRAMT_AVG | 신용 일시불 이용금액 평균 |
| CSADVC_USE_GRAMT_AVG | 현금서비스 이용금액 평균 |
| CARD_SPM_USE_GRAMT_AVG | 카드 할부 이용금액 평균 |
| LON_OWNR_NOPE | 대출 보유 인원 |
| BANK_LON_OWRN_NOPE | 은행 대출 인원 |
| CR_LON_OWNR_NOPE | 신용 대출 인원 |
| HOUSE_LON_OWRN_NOPE | 주택담보대출 인원 |
| LON_AVG_BLC | 대출 평균 잔액 |
| BANK_AVG_LON_BLC | 은행 대출 평균 잔액 |
| CR_AVG | 평균 신용등급 |
| ECNM_ACTV_PPLTN_CNT | 경제활동인구 수 |
| APT_PPLTN_CNT / NAPT_PPLTN_CNT | 아파트/비아파트 거주 인구 |
| HOME_PPLTN_CNT / NHOME_PPLTN_CNT | 자가/비자가 인구 |
| YR_HIGHINCM_AVG | 연간 고소득 평균 |
| YR_INCM_RATE_* | 연소득 구간별 비율 |

### 2-2. AS_KCB_CREDTINFO2 (PIA 암호화)
> .pia 확장자 - 개인정보 암호화 파일. 복호화 키 필요.

### 2-3. AS_KCB_CREDTINFO3 (읍면동-연령별 신용정보 요약)
> AS_KCB_CREDTINFO와 유사 구조, 성별 구분 없이 읍면동-연령대별 집계. STDG_CD(법정동코드) 사용.

### 2-4. KCB_TRANSF_PART (변환 파티션 정보)

| 컬럼명 | 설명 |
|--------|------|
| TRNSF_DVS | 변환 구분 |
| LC_DVS | 지역 구분 |
| CRTR_DVS | 기준 구분 |
| PART_VL | 파티션 값 |
| TM_PART_AMT | 파티션 금액 |

---

## 3. 내비게이션 (T맵 OD) 데이터

### 3-1. as_tmap_od (T맵 출발-도착 통행 데이터)
> 일별 출발지-목적지(POI) 방문 통행 데이터. 아산시 목적지 기준.

| 컬럼명 | 설명 | 예시 |
|--------|------|------|
| drv_ymd | 운행일자 (YYYYMMDD) | 20190201 |
| frst_dptre_ctpv_nm | 최초 출발 시도명 | 충청남도 |
| frst_dptre_sgg_nm | 최초 출발 시군구명 | 천안시 서북구 |
| dstn_nm | 목적지명 (POI) | 삼성디스플레이/아산캠퍼스 |
| dstn_coord_x | 목적지 경도 (X) | 127.064 |
| dstn_coord_y | 목적지 위도 (Y) | 36.814 |
| dstn_ctpv_nm | 목적지 시도명 | 충청남도 |
| dstn_sgg_nm | 목적지 시군구명 | 아산시 |
| dstn_dong_nm | 목적지 읍면동명 | 탕정면 |
| dstn_addr | 목적지 주소 | 충남 아산시 탕정면 명암리 |
| dstn_ctgy | 목적지 카테고리 | 기업/단체_주요그룹_삼성 |
| vst_cnt | 방문 건수 | 35 |
| ntv_vst_cnt | 내국인 방문 건수 | 4 |
| fm_user_cnt | 여성 이용자 수 | 8 |
| ml_user_cnt | 남성 이용자 수 | 10 |
| sx_abs_user_cnt | 성별 미상 이용자 수 | 17 |
| twt_les_user_cnt | 20대 이하 이용자 수 | 0 |
| trt_user_cnt | 30대 이용자 수 | 14 |
| frt_user_cnt | 40대 이용자 수 | 3 |
| fft_user_cnt | 50대 이용자 수 | 0 |
| sxt_abv_user_cnt | 60대 이상 이용자 수 | 1 |
| age_abs_user_cnt | 연령 미상 이용자 수 | 17 |
| avg_drv_min | 평균 운행시간 (분) | 29.2 |
| avg_drv_dstc | 평균 운행거리 (km) | 10.6 |
| avg_stay_min | 평균 체류시간 (분) | 353.3 |
| frst_arvl_hr | 1순위 도착 시간대 | 8 |
| scnd_arvl_hr | 2순위 도착 시간대 | 7 |
| thrd_arvl_hr | 3순위 도착 시간대 | 5 |
| frst_next_dstn_nm | 1순위 다음 목적지 | 이마트/천안점 |
| scnd_next_dstn_nm | 2순위 다음 목적지 | 버팀병원/오산점 |
| thrd_next_dstn_nm | 3순위 다음 목적지 | 심미치과의원 |

---

## 4. 관광데이터랩 데이터 (이미 DB 적재 완료)

> `asan_media` DB, `asan_media` 스키마에 적재됨

| 테이블 | 설명 | 기간 |
|--------|------|------|
| visitor_domestic_trend | 내국인 월별 방문자수 (KT) | 2018~2026.03 |
| visitor_domestic_by_region | 내국인 읍면동별 방문자수 | 연도별 |
| visitor_domestic_origin | 내국인 방문자 거주지 비율 | 연도별 |
| visitor_foreign_trend | 외국인 월별 방문자수 (SKT) | 2020~2026.03 |
| visitor_foreign_by_region | 외국인 읍면동별 방문자수 | 연도별 |
| visitor_foreign_origin | 외국인 출신 국가별 비율 | 연도별 |
| spending_trend | 관광소비액 월별 추이 | 2018~2026.02 |
| spending_by_industry | 업종별 관광지출 비율 | 연도별 |
| spending_by_region | 읍면동별 관광지출 비율 | 연도별 |

---

## 약어 정리

| 약어 | 의미 |
|------|------|
| CRTR_YM | 기준년월 (CRiTeRia Year Month) |
| CCND | 카드 (Card Connected) |
| CUST | 고객 (Customer) |
| FRCS | 가맹점 (Franchise) |
| CSPT | 소비 (Consumption) |
| CTPV | 시도 (City/Province) |
| SGG | 시군구 |
| DONG | 읍면동 |
| TOBIZ | 업종 (Type of Business) |
| NOCS | 건수 (Number of Cases) |
| AMT | 금액 (Amount) |
| ML/FM | 남성/여성 |
| TWT/TRT/FRT/FFT/SXT/SVT | 20대/30대/40대/50대/60대/70대+ |
| EXCL_LC | 지역화폐 제외 (Exclude Local Currency) |
| EXCL_RSDT | 비거주자 (Exclude Resident) |
| DSST_SPRT | 재난지원 (Disaster Support) |
| TCBIZ/CLSBIZ | 개업/폐업 |
| SLS_SZ | 매출규모 (Sales Size) |
| WEEK | 주간 (CRTR_WEEK: 기준주차) |
