# 아산시 방송 홍보 효과 분석

방송 홍보에 따른 아산시 관광/축제 방문객 및 경제적 파급효과 분석 프로젝트

## 구조

```
asan_media/
├── sql/
│   └── schema.sql          # PostgreSQL 테이블 DDL
├── etl/
│   └── load_tourism_data.py # ZIP → CSV 파싱 → PG 적재 스크립트
├── config/
│   └── db_config.yaml       # DB 접속 설정 (템플릿)
└── README.md
```

## 데이터 소스

| 카테고리 | 출처 | 기간 |
|---------|------|------|
| 지역별 방문자수 (내국인/KT) | 한국관광공사 관광데이터랩 | 2018.01~2026.03 |
| 지역별 방문자수 (외국인/SKT) | 한국관광공사 관광데이터랩 | 2020.01~2026.03 |
| 지역별 관광지출액 (내국인/외지인) | 한국관광공사 관광데이터랩 | 2018.01~2026.02 |
| 지역별 관광지출액 (내국인/현지인) | 한국관광공사 관광데이터랩 | 2018.01~2026.02 |
| 지역별 관광지출액 (외국인) | 한국관광공사 관광데이터랩 | 2018.01~2026.02 |

## 사용법

```bash
# 1. DB 설정
cp config/db_config.yaml config/db_config.local.yaml
# db_config.local.yaml 수정

# 2. 스키마 생성
psql -f sql/schema.sql

# 3. 데이터 적재
python etl/load_tourism_data.py --data-dir /path/to/데이터 --config config/db_config.local.yaml
```
