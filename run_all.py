"""
전체 파이프라인 실행
==================
python run_all.py

필수 패키지:
  pip install pandas numpy matplotlib tqdm statsmodels requests python-dotenv reportlab

선택 패키지:
  pip install google-api-python-client causalimpact
"""
import subprocess
import sys
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

steps = [
    ("Step 1: 카드매출 베이스라인", "step1_card_baseline.py"),
    ("Step 2: T맵 관광 분석", "step2_tmap_tourism.py"),
    ("Step 3: 인과추론", "step3_causal_inference.py"),
    ("Step 4: 시각화", "step4_visualization.py"),
    ("Step 5: 경제적 파급효과", "step5_economic_impact.py"),
    ("Step 6: 온라인 버즈 수집", "step6_online_buzz.py"),
    ("Step 7: 교란요소 분석", "step7_confounders.py"),
    ("Step 8: 종합 리포트", "step8_final_report.py"),
]

print("=" * 60)
print("아산시 방송 홍보 효과 분석 - 전체 파이프라인")
print("=" * 60)

for title, script in steps:
    print(f"\n{'#' * 60}")
    print(f"# {title}")
    print(f"{'#' * 60}")
    result = subprocess.run([sys.executable, script], capture_output=False)
    if result.returncode != 0:
        print(f"\n[!] {title} 실패 (exit code: {result.returncode})")
        resp = input("계속 진행? (y/n): ").strip().lower()
        if resp != 'y':
            break

print("\n\n" + "=" * 60)
print("전체 파이프라인 완료!")
print("결과 폴더: C:\\Users\\HP\\Desktop\\02.분석결과")
print("=" * 60)
