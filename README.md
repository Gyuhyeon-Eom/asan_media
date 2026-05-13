# 아산시 방송 홍보 효과 분석

방송 프로그램 7건의 홍보 효과를 다각도로 분석하는 파이프라인.

## 분석 대상 방송
| 방송명 | 방송사 | 방영일 | 시청률 |
|--------|--------|--------|--------|
| 전국노래자랑 | KBS1 | 2025-06-08 | 6.5% |
| 전현무계획2 | MBN | 2025-11-07 | 1.5% |
| 굿모닝대한민국 | KBS2 | 2025-11-12 | 0.55% |
| 6시내고향 | KBS1 | 2025-11-13 | 5.5% |
| 같이삽시다3 | KBS2 | 2025-11-24 | 3.0% |
| 뛰어야산다2 | MBN | 2026-01-12 | 1.5% |
| 황제파워 | SBS FM | 2026-05-09 | - |

## 파이프라인 구조
```
Step 1: 카드매출 베이스라인 (아산페이)
Step 2: T맵 관광지 방문 분석
Step 3: 인과추론 (DID)
Step 4: 시각화
Step 5: 경제적 파급효과
Step 6: 온라인 버즈 (네이버 블로그/뉴스 + DataLab + YouTube)
Step 7: 교란요소 (날씨/공휴일/시즌/이벤트)
Step 8: 종합 PDF 리포트
```

## 설치 & 실행
```bash
pip install -r requirements.txt
cp .env.example .env  # API 키 설정
python run_all.py      # 전체 실행
```

개별 step 실행:
```bash
python step6_online_buzz.py   # 온라인 버즈만
python step7_confounders.py   # 교란요소만
python step8_final_report.py  # 리포트만
```

## 데이터 소스
- **T맵**: 관광지 목적지 검색 (2019~2026)
- **SKT**: 읍면동별 유동인구 (2026-01~02)
- **아산페이**: 카드매출 (2026-01~02)
- **네이버**: 블로그/뉴스 검색 API + DataLab 검색 트렌드
- **YouTube**: Data API v3 (영상/댓글)
- **날씨**: Open-Meteo (무료, API키 불필요)
- **공휴일/이벤트**: 수동 정의 (config.py)

## 설정
- `config.py`: 경로, 방송 이벤트, 관광지 매핑
- `.env`: API 키 (git 제외)
