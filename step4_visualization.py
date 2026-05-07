"""
Step 4: 시각화 - 방송효과 대시보드
================================
- 방송 이벤트 타임라인 + 시계열 오버레이
- 전후 비교 차트
- 공간 히트맵 (읍면동별)

실행: python step4_visualization.py
선행: step1~3 결과 파일 필요
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import font_manager, rc
import warnings
warnings.filterwarnings('ignore')

from config import *

# 한글 폰트 설정 (Windows)
try:
    font_path = "C:/Windows/Fonts/malgun.ttf"
    font_name = font_manager.FontProperties(fname=font_path).get_name()
    rc('font', family=font_name)
except:
    plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False

COLORS = ['#2196F3', '#FF5722', '#4CAF50', '#FFC107', '#9C27B0', '#00BCD4', '#E91E63']

print("=" * 60)
print("Step 4: 시각화")
print("=" * 60)


# ============================================================
# 1. 데이터 로드
# ============================================================
dfs = {}
for name, fname, date_col in [
    ('card_dong', 'card_outsider_by_dong_monthly.csv', 'CRTR_YM'),
    ('card_daily', 'card_outsider_daily.csv', 'SALE_DATE'),
    ('tmap_poi', 'tmap_poi_daily.csv', 'drv_ymd'),
    ('tmap_tourism', 'tmap_tourism_daily.csv', 'drv_ymd'),
    ('tmap_origin', 'tmap_poi_origin_monthly.csv', None),
    ('stl', 'stl_outsider_total.csv', 'date'),
]:
    try:
        df = pd.read_csv(OUTPUT_DIR / fname, encoding='utf-8-sig')
        if date_col and date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col].astype(str).str[:10])
        dfs[name] = df
        print(f"  {name}: {len(df):,}행")
    except FileNotFoundError:
        print(f"  {name}: 파일 없음")


# ============================================================
# 2. 메인 차트: 외지인 매출 + 방송 이벤트 타임라인
# ============================================================
def plot_timeline_with_broadcasts(df, date_col, value_col, title, filename):
    """시계열 + 방송 이벤트 수직선"""
    fig, ax = plt.subplots(figsize=(16, 6))
    ax.plot(df[date_col], df[value_col], color='#2196F3', linewidth=1.5, alpha=0.8)

    # 이동평균
    if len(df) > 30:
        ma = df[value_col].rolling(window=7, min_periods=1).mean()
        ax.plot(df[date_col], ma, color='#FF5722', linewidth=2, label='7일 이동평균')

    # 방송 이벤트 표시
    y_max = df[value_col].max()
    for i, b in enumerate(BROADCASTS):
        air = pd.Timestamp(b['air_date'])
        if df[date_col].min() <= air <= df[date_col].max():
            color = COLORS[i % len(COLORS)]
            ax.axvline(x=air, color=color, linestyle='--', alpha=0.7, linewidth=1.5)
            ax.text(air, y_max * (0.95 - i * 0.08),
                    f" {b['name']}\n ({b['air_date']})",
                    fontsize=7, color=color, rotation=0, va='top')

    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xlabel('날짜')
    ax.set_ylabel(value_col)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    plt.xticks(rotation=45)
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / filename, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  저장: {filename}")

# 2-1. 카드매출 월별 타임라인
if 'card_dong' in dfs:
    ts = dfs['card_dong'].groupby('CRTR_YM')['outsider_amt'].sum().reset_index()
    ts.columns = ['date', 'outsider_amt']
    ts['date'] = pd.to_datetime(ts['date'].astype(str), format='%Y%m')
    plot_timeline_with_broadcasts(ts, 'date', 'outsider_amt',
                                  '아산시 외지인 카드매출 추이 + 방송 이벤트',
                                  'fig_card_timeline.png')

# 2-2. T맵 관광 일별 타임라인
if 'tmap_tourism' in dfs:
    plot_timeline_with_broadcasts(dfs['tmap_tourism'], 'drv_ymd', 'total_visits',
                                  '아산시 T맵 관광지 방문 추이 + 방송 이벤트',
                                  'fig_tmap_timeline.png')


# ============================================================
# 3. 프로그램별 전후 비교 (Bar chart)
# ============================================================
def plot_pre_post_bars(filename='fig_pre_post_comparison.png'):
    """방송 전후 4주 외지인 매출/방문 비교"""
    try:
        df_summary = pd.read_csv(OUTPUT_DIR / "tmap_broadcast_effect_summary.csv", encoding='utf-8-sig')
    except FileNotFoundError:
        print("  tmap_broadcast_effect_summary.csv 없음")
        return

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # 전후 비교 (절대값)
    x = range(len(df_summary))
    w = 0.35
    axes[0].bar([i - w/2 for i in x], df_summary['pre_daily_avg'], w, label='방송 전 4주', color='#90CAF9')
    axes[0].bar([i + w/2 for i in x], df_summary['post_daily_avg'], w, label='방송 후 4주', color='#FF8A65')
    axes[0].set_xticks(list(x))
    axes[0].set_xticklabels(df_summary['broadcast'], rotation=45, ha='right', fontsize=8)
    axes[0].set_title('방송 전후 일평균 관광지 방문 (T맵)', fontweight='bold')
    axes[0].set_ylabel('일평균 방문')
    axes[0].legend()
    axes[0].grid(True, alpha=0.3, axis='y')

    # 변화율
    colors = ['#4CAF50' if v > 0 else '#F44336' for v in df_summary['change_pct'].fillna(0)]
    axes[1].barh(df_summary['broadcast'], df_summary['change_pct'].fillna(0), color=colors)
    axes[1].axvline(x=0, color='black', linewidth=0.5)
    axes[1].set_title('방송 전후 변화율 (%)', fontweight='bold')
    axes[1].set_xlabel('변화율 (%)')
    axes[1].grid(True, alpha=0.3, axis='x')

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / filename, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  저장: {filename}")

plot_pre_post_bars()


# ============================================================
# 4. 관광지별 시계열 (방송 노출 POI별)
# ============================================================
def plot_poi_timeseries(filename='fig_poi_timeseries.png'):
    if 'tmap_poi' not in dfs:
        return

    df = dfs['tmap_poi']
    pois = df['matched_poi'].value_counts().head(6).index.tolist()

    fig, axes = plt.subplots(len(pois), 1, figsize=(16, 3 * len(pois)), sharex=True)
    if len(pois) == 1:
        axes = [axes]

    for idx, poi in enumerate(pois):
        ax = axes[idx]
        ts = df[df['matched_poi'] == poi].sort_values('drv_ymd')
        ax.plot(ts['drv_ymd'], ts['visit_cnt'], alpha=0.4, color='#2196F3', linewidth=0.8)

        # 7일 이동평균
        if len(ts) > 7:
            ma = ts['visit_cnt'].rolling(7, min_periods=1).mean()
            ax.plot(ts['drv_ymd'], ma, color='#FF5722', linewidth=2)

        # 방송 이벤트
        for b in BROADCASTS:
            for loc in b['locations']:
                if poi.lower() in loc.lower() or any(kw in loc for kw in BROADCAST_POI_KEYWORDS.get(poi, [])):
                    air = pd.Timestamp(b['air_date'])
                    ax.axvline(x=air, color='red', linestyle='--', alpha=0.8)
                    ax.text(air, ax.get_ylim()[1] * 0.9, b['name'][:6],
                            fontsize=7, color='red', rotation=90, va='top')

        ax.set_title(f'{poi}', fontsize=11, fontweight='bold')
        ax.grid(True, alpha=0.3)

    plt.xlabel('날짜')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / filename, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  저장: {filename}")

plot_poi_timeseries()


# ============================================================
# 5. STL 분해 시각화
# ============================================================
def plot_stl(filename='fig_stl_decomposition.png'):
    if 'stl' not in dfs:
        return

    df = dfs['stl']
    fig, axes = plt.subplots(4, 1, figsize=(16, 10), sharex=True)

    for ax, col, title in zip(axes,
                               ['observed', 'trend', 'seasonal', 'residual'],
                               ['관측값 (외지인 매출)', '추세', '계절성', '잔차 (이상치)']):
        ax.plot(df['date'], df[col], color='#2196F3', linewidth=1.5)
        ax.set_title(title, fontsize=11, fontweight='bold')
        ax.grid(True, alpha=0.3)

        if col == 'residual':
            # 2sigma 밴드
            mu, sigma = df[col].mean(), df[col].std()
            ax.axhline(y=mu + 2*sigma, color='red', linestyle='--', alpha=0.5, label='+2σ')
            ax.axhline(y=mu - 2*sigma, color='red', linestyle='--', alpha=0.5, label='-2σ')
            anomalies = df[abs(df[col] - mu) > 2 * sigma]
            ax.scatter(anomalies['date'], anomalies[col], color='red', s=50, zorder=5, label='이상치')
            ax.legend(fontsize=8)

        # 방송 이벤트
        for b in BROADCASTS:
            air = pd.Timestamp(b['air_date'])
            if pd.Timestamp(df['date'].min()) <= air <= pd.Timestamp(df['date'].max()):
                ax.axvline(x=air, color='#FF5722', linestyle=':', alpha=0.4)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / filename, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  저장: {filename}")

plot_stl()


# ============================================================
# 6. 출발지 히트맵 (시도별 유입)
# ============================================================
def plot_origin_heatmap(filename='fig_origin_heatmap.png'):
    if 'tmap_origin' not in dfs:
        return

    df = dfs['tmap_origin']
    pivot = df.pivot_table(values='visit_cnt', index='frst_dptre_ctpv_nm',
                           columns='ym', aggfunc='sum', fill_value=0)
    # 상위 15개 시도만
    top = pivot.sum(axis=1).nlargest(15).index
    pivot = pivot.loc[top]

    fig, ax = plt.subplots(figsize=(16, 8))
    im = ax.imshow(pivot.values, cmap='YlOrRd', aspect='auto')
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=90, fontsize=7)
    ax.set_title('출발지(시도)별 아산 관광지 방문 추이', fontsize=14, fontweight='bold')
    plt.colorbar(im, ax=ax, label='방문 수')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / filename, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  저장: {filename}")

plot_origin_heatmap()


# ============================================================
# 7. 종합 리포트 요약
# ============================================================
print("\n--- 생성된 차트 ---")
import glob as g
charts = g.glob(str(OUTPUT_DIR / "fig_*.png"))
for c in sorted(charts):
    print(f"  {os.path.basename(c)}")

print("\n" + "=" * 60)
print("Step 4 완료! 결과:", OUTPUT_DIR)
print("=" * 60)
