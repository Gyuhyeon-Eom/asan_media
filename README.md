# 아산시 방송 홍보 효과 분석

## 구조

```
analysis/
  config.py              # 설정 (경로, 방송 이벤트, 업종 코드 등)
  utils.py               # 공통 유틸리티
  step1_card_baseline.py  # 카드매출 베이스라인 구축
  step2_tmap_tourism.py   # T맵 관광지 방문 분석
  step3_causal_inference.py  # 인과추론 (STL/DID/CausalImpact)
  step4_visualization.py  # 시각화
  step5_economic_impact.py  # 경제적 파급효과 산출
  run_all.py              # 전체 실행
```

## 실행 방법

```bash
# 1. 필수 패키지 설치
pip install pandas numpy matplotlib tqdm statsmodels

# 2. 선택 패키지 (CausalImpact)
pip install causalimpact

# 3. 전체 실행
cd analysis
python run_all.py

# 또는 단계별 실행
python step1_card_baseline.py
python step2_tmap_tourism.py
python step3_causal_inference.py
python step4_visualization.py
python step5_economic_impact.py
```

## 경로 설정

`config.py`에서 회사 PC 경로를 확인:
- `DATA_DIR`: 원본 데이터 (C:\Users\HP\Desktop\01.데이터)
- `OUTPUT_DIR`: 결과 저장 (C:\Users\HP\Desktop\02.분석결과)

## 분석 방법론

### 1. 베이스라인 구축 (Step 1-2)
- 카드매출 외지인 소비 시계열 (2019~)
- T맵 관광지 방문 시계열 (2019~)
- 읍면동/업종/출발지별 세분화

### 2. 교란변수 제거 (Step 3)
- STL 계절 분해 -> 계절성 제거 후 잔차에서 이상치 탐지
- DID (이중차분법) -> 방송 노출 vs 미노출 읍면동 비교
- CausalImpact -> "방송이 없었다면" 합성 시나리오와 비교

### 3. 경제효과 환산 (Step 5)
- 순수 방문객 증가 x 1인당 관광소비 = 직접 효과
- 산업연관 승수 적용 -> 총 경제효과
- 프로그램별 ROI

## 산출물

### CSV
| 파일 | 내용 |
|-----|------|
| card_outsider_by_dong_monthly.csv | 읍면동별 외지인 월별 매출 |
| card_outsider_daily.csv | 일별 외지인 매출 |
| card_did_panel_monthly.csv | DID용 처치/대조군 패널 |
| card_outsider_origin_monthly.csv | 출발지별 유입 소비 |
| tmap_poi_daily.csv | 관광지별 일별 방문 |
| tmap_poi_origin_daily.csv | 관광지별 출발지 방문 |
| tmap_broadcast_effect_summary.csv | 방송 전후 비교 요약 |
| did_results.csv | DID 인과추론 결과 |
| economic_impact_by_broadcast.csv | 프로그램별 경제효과 |

### 차트
| 파일 | 내용 |
|-----|------|
| fig_card_timeline.png | 외지인 매출 + 방송 타임라인 |
| fig_tmap_timeline.png | T맵 관광 방문 + 방송 타임라인 |
| fig_pre_post_comparison.png | 방송 전후 비교 바 차트 |
| fig_poi_timeseries.png | 관광지별 시계열 |
| fig_stl_decomposition.png | STL 계절 분해 |
| fig_origin_heatmap.png | 출발지 히트맵 |
