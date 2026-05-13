#!/usr/bin/env python3
"""
아산시 방송 홍보효과 비교분석 - 3가지 방법론 적용
(1) Bayesian CausalImpact  (2) 전년 동기 DID  (3) 공변량 DID
"""
import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import TwoSlopeNorm
import statsmodels.api as sm
from causal_impact import CausalImpact
import base64
from pathlib import Path

plt.rcParams['font.family'] = 'AppleGothic'
plt.rcParams['axes.unicode_minus'] = False

OUT = Path('/Users/eomgyuhyeon/.openclaw/workspace/아산시/media/analysis/output')
OUT.mkdir(parents=True, exist_ok=True)

# ── 1. T맵 데이터 로드 ──
CSV_FILES = {
    '2025-01': '/Users/eomgyuhyeon/.openclaw/media/inbound/4f04101b-ae3c-4f92-b99f-ca5a8dac04fb.csv',
    '2025-02': '/Users/eomgyuhyeon/.openclaw/media/inbound/c8269447-3eb5-4418-aec1-faca498dce7d.csv',
    '2025-03': '/Users/eomgyuhyeon/.openclaw/media/inbound/1ded847e-9734-4bdc-9da9-662c393290f1.csv',
    '2025-04': '/Users/eomgyuhyeon/.openclaw/media/inbound/5fd1759a-7eed-45df-a0d4-736e7df21fe7.csv',
    '2025-11': '/Users/eomgyuhyeon/.openclaw/media/inbound/38c4cf8d-2241-4447-8176-22935c9def22.csv',
    '2025-12': '/Users/eomgyuhyeon/.openclaw/media/inbound/d8cac458-ac19-47f9-b8a4-0ed68ae7ce04.csv',
    '2026-01': '/Users/eomgyuhyeon/.openclaw/media/inbound/28ec64fd-a593-42cc-9530-1fc2d2a1b441.csv',
    '2026-02': '/Users/eomgyuhyeon/.openclaw/media/inbound/aab2bac5-2d74-419c-848a-b4afd07914c2.csv',
    '2026-03': '/Users/eomgyuhyeon/.openclaw/media/inbound/7c46f8fc-aa33-4548-8268-663c622058dd.csv',
}

dfs = []
for label, fpath in CSV_FILES.items():
    df = pd.read_csv(fpath, low_memory=False)
    df.columns = [c.lower() for c in df.columns]
    dfs.append(df)
tmap = pd.concat(dfs, ignore_index=True)
tmap['date'] = pd.to_datetime(tmap['drv_ymd'], format='%Y%m%d')
tmap['vst_cnt'] = pd.to_numeric(tmap['vst_cnt'], errors='coerce').fillna(0).astype(int)

print(f"T맵 전체: {len(tmap):,} rows, {tmap['date'].min().date()} ~ {tmap['date'].max().date()}")

# ── 관광지 분류 ──
SITE_KEYWORDS = {
    '신정호': ['신정호'],
    '곡교천': ['곡교천', '은행나무'],
    '현충사': ['현충사'],
    '온양온천': ['온양온천'],
    '외암민속마을': ['외암민속마을', '외암'],
    '도고': ['도고', '도고온천', '도고파라다이스', '파라다이스스파도고'],
    '영인산': ['영인산'],
    '피나클랜드': ['피나클랜드'],
}

def classify_site(dstn_nm):
    if pd.isna(dstn_nm):
        return None
    name = str(dstn_nm)
    for site, keywords in SITE_KEYWORDS.items():
        for kw in keywords:
            if kw in name:
                return site
    return None

tmap['site'] = tmap['dstn_nm'].apply(classify_site)
tmap_sites = tmap[tmap['site'].notna()].copy()

daily = tmap_sites.groupby(['date', 'site'])['vst_cnt'].sum().reset_index()
daily_pivot = daily.pivot_table(index='date', columns='site', values='vst_cnt', fill_value=0)

daily_total = tmap.groupby('date')['vst_cnt'].sum().reset_index()
daily_total.columns = ['date', 'total_visits']
daily_total = daily_total.set_index('date').sort_index()

ALL_SITES = sorted(daily_pivot.columns.tolist())
print(f"관광지: {ALL_SITES}")

# ── 교란요소 ──
conf = pd.read_csv(OUT / 'confounders_merged.csv')
conf['date'] = pd.to_datetime(conf['date'])
conf = conf.set_index('date').sort_index()

# ── 방송 정보 ──
BROADCASTS = {
    '전국노래자랑': {'date': '2025-06-08', 'channel': 'KBS1', 'rating': 6.5, 'budget': 3518, 'sites': ['신정호'], 'tmap_possible': False},
    '전현무계획2': {'date': '2025-11-07', 'channel': 'MBN', 'rating': 1.5, 'budget': 50000, 'sites': 'all', 'tmap_possible': True},
    '굿모닝대한민국': {'date': '2025-11-12', 'channel': 'KBS2', 'rating': 0.55, 'budget': 20000, 'sites': ['온양온천', '곡교천', '현충사', '피나클랜드'], 'tmap_possible': True},
    '6시내고향': {'date': '2025-11-13', 'channel': 'KBS1', 'rating': 5.5, 'budget': 110000, 'sites': 'all', 'tmap_possible': True},
    '같이삽시다3': {'date': '2025-11-24', 'end_date': '2025-12-15', 'channel': 'KBS2', 'rating': 3.0, 'budget': 133000, 'sites': ['곡교천', '신정호', '영인산', '외암민속마을', '도고'], 'tmap_possible': True},
    '뛰어야산다2': {'date': '2026-01-12', 'channel': 'MBN', 'rating': 1.5, 'budget': 45000, 'sites': ['신정호', '곡교천', '현충사', '온양온천'], 'tmap_possible': True},
    '황제파워': {'date': '2026-05-09', 'channel': 'SBS FM', 'rating': None, 'budget': 220000, 'sites': ['온양온천'], 'tmap_possible': False},
}

def get_control_sites(treated):
    if treated == 'all':
        return []
    return [s for s in ALL_SITES if s not in treated]

# ── 방법 1: CausalImpact ──
def run_causal_impact(name, info, pre_days=28, post_days=28):
    if info['sites'] == 'all':
        return None
    bdate = pd.Timestamp(info['date'])
    treated = info['sites']
    control = get_control_sites(treated)
    if not control:
        return None

    ts = daily_pivot.copy().sort_index().asfreq('D', fill_value=0)
    y = ts[treated].sum(axis=1).astype(float)
    x = ts[control].sum(axis=1).astype(float)
    ci_data = pd.DataFrame({'y': y, 'x1': x})

    pre_start = bdate - pd.Timedelta(days=pre_days)
    post_end = bdate + pd.Timedelta(days=post_days)
    ci_data = ci_data[(ci_data.index >= pre_start) & (ci_data.index <= post_end)]
    if len(ci_data) < 14:
        return None

    try:
        ci = CausalImpact(ci_data, bdate, n_seasons=7)
        ci.run()
        r = ci.result
        inter_idx = ci._inter_index  # integer index of intervention

        # Restore date index for charting
        original_dates = ci_data.index
        r_dates = original_dates[:len(r)]  # align dates
        r = r.copy()
        r.index = r_dates

        post_r = r.iloc[inter_idx:]
        pre_r = r.iloc[:inter_idx]

        avg_actual = post_r['y'].mean()
        avg_pred = post_r['pred'].mean()
        avg_effect = avg_actual - avg_pred
        avg_effect_rel = avg_effect / avg_pred if avg_pred != 0 else 0

        cum_effect = post_r['cum_impact'].iloc[-1] if len(post_r) > 0 else 0
        cum_pred = post_r['pred'].sum()
        cum_effect_rel = cum_effect / cum_pred if cum_pred != 0 else 0

        avg_ci_lower = post_r['pred_diff_conf_int_lower'].mean()
        avg_ci_upper = post_r['pred_diff_conf_int_upper'].mean()

        cum_ci_lower = post_r['cum_impact_conf_int_lower'].iloc[-1] if len(post_r) > 0 else 0
        cum_ci_upper = post_r['cum_impact_conf_int_upper'].iloc[-1] if len(post_r) > 0 else 0
        significant = (cum_ci_lower > 0) or (cum_ci_upper < 0)

        fig, axes = plt.subplots(3, 1, figsize=(10, 8), sharex=True)
        fig.suptitle(f'CausalImpact: {name}', fontsize=14, fontweight='bold')

        ax = axes[0]
        ax.plot(r.index, r['pred'], color='#2196F3', ls='--', label='예측(반사실)')
        ax.fill_between(r.index, r['pred_conf_int_lower'], r['pred_conf_int_upper'], alpha=0.15, color='#2196F3')
        ax.plot(r.index, r['y'], color='#333', lw=1.2, label='실제')
        ax.axvline(bdate, color='red', ls='--', alpha=0.7, label='방송일')
        ax.set_ylabel('일별 방문수')
        ax.legend(fontsize=8, loc='upper left')
        ax.set_title('실제 vs 반사실 예측', fontsize=10)

        ax = axes[1]
        ax.plot(r.index, r['pred_diff'], color='#4CAF50', lw=1)
        ax.fill_between(r.index, r['pred_diff_conf_int_lower'], r['pred_diff_conf_int_upper'], alpha=0.15, color='#4CAF50')
        ax.axhline(0, color='gray', ls='-', alpha=0.3)
        ax.axvline(bdate, color='red', ls='--', alpha=0.7)
        ax.set_ylabel('개별 효과')
        ax.set_title('포인트별 인과 효과 (실제-예측)', fontsize=10)

        ax = axes[2]
        ax.plot(r.index, r['cum_impact'], color='#FF9800', lw=1)
        ax.fill_between(r.index, r['cum_impact_conf_int_lower'], r['cum_impact_conf_int_upper'], alpha=0.15, color='#FF9800')
        ax.axhline(0, color='gray', ls='-', alpha=0.3)
        ax.axvline(bdate, color='red', ls='--', alpha=0.7)
        ax.set_ylabel('누적 효과')
        ax.set_title('누적 인과 효과', fontsize=10)

        plt.tight_layout()
        chart_path = OUT / f'ci_{name}.png'
        fig.savefig(chart_path, dpi=150, bbox_inches='tight')
        plt.close(fig)

        result = {
            'method': 'CausalImpact',
            'name': name,
            'avg_actual': float(avg_actual),
            'avg_predicted': float(avg_pred),
            'avg_effect_abs': float(avg_effect),
            'avg_effect_rel': float(avg_effect_rel),
            'cum_effect': float(cum_effect),
            'cum_effect_rel': float(cum_effect_rel),
            'ci_lower': float(avg_ci_lower),
            'ci_upper': float(avg_ci_upper),
            'cum_ci_lower': float(cum_ci_lower),
            'cum_ci_upper': float(cum_ci_upper),
            'significant': significant,
            'chart': str(chart_path),
        }
        print(f"  CI {name}: avg={avg_effect_rel:.1%}, cum={cum_effect:.0f}, sig={significant}")
        return result
    except Exception as e:
        print(f"  CI {name}: ERROR - {e}")
        import traceback; traceback.print_exc()
        return None

# ── 방법 2: 전년 동기 DID ──
def run_yoy_did(name, info):
    if name != '뛰어야산다2':
        return None
    treated = info['sites']
    control = get_control_sites(treated)
    if not control:
        return None

    ts = daily_pivot.copy().sort_index()
    # 2026-01-12~03-31 vs 2025-01-12~03-31
    prev = ts[(ts.index >= '2025-01-12') & (ts.index <= '2025-03-31')]
    curr = ts[(ts.index >= '2026-01-12') & (ts.index <= '2026-03-31')]
    if len(prev) == 0 or len(curr) == 0:
        return None

    tp = prev[treated].sum().sum()
    tc = curr[treated].sum().sum()
    cp = prev[control].sum().sum()
    cc = curr[control].sum().sum()
    t_chg = (tc - tp) / tp if tp else 0
    c_chg = (cc - cp) / cp if cp else 0
    did = t_chg - c_chg

    result = {
        'method': 'YoY DID', 'name': name,
        'treat_prev': int(tp), 'treat_curr': int(tc), 'treat_change_pct': t_chg,
        'ctrl_prev': int(cp), 'ctrl_curr': int(cc), 'ctrl_change_pct': c_chg,
        'did_effect': did, 'significant': abs(did) > 0.05,
    }
    print(f"  YoY DID {name}: treat={t_chg:.1%}, ctrl={c_chg:.1%}, DID={did:.1%}")
    return result

# ── 방법 3: 공변량 DID / Pre-Post ──
def run_covariate_did(name, info, pre_days=28, post_days=28):
    bdate = pd.Timestamp(info['date'])
    pre_start = bdate - pd.Timedelta(days=pre_days)
    post_end = bdate + pd.Timedelta(days=post_days)
    is_all = (info['sites'] == 'all')
    cov_cols = ['temperature_2m_mean', 'precipitation_sum', 'is_weekend', 'is_holiday', 'season_score']

    if is_all:
        ts = daily_total.copy()
        ts = ts[(ts.index >= pre_start) & (ts.index <= post_end)]
        if len(ts) < 14:
            return None
        merged = ts.join(conf, how='left')
        merged['post'] = (merged.index >= bdate).astype(int)
        y = merged['total_visits']
        x_cols = ['post'] + [c for c in cov_cols if c in merged.columns]
        X = sm.add_constant(merged[x_cols].fillna(0))
        try:
            model = sm.OLS(y, X).fit(cov_type='HC1')
            pc = float(model.params.get('post', 0))
            pp = float(model.pvalues.get('post', 1))
            ci = model.conf_int().loc['post']
            bl = float(y[merged['post'] == 0].mean())
            return {
                'method': 'Covariate Pre-Post', 'name': name,
                'post_effect': pc, 'post_pvalue': pp,
                'ci_lower': float(ci[0]), 'ci_upper': float(ci[1]),
                'r_squared': float(model.rsquared), 'n_obs': int(model.nobs),
                'significant': pp < 0.1, 'baseline_mean': bl,
                'effect_pct': pc / bl if bl else 0,
            }
        except Exception as e:
            print(f"  CovPrePost {name}: ERROR - {e}"); return None
    else:
        treated = info['sites']
        control = get_control_sites(treated)
        if not control:
            return None
        ts = daily_pivot.copy().sort_index()
        ts = ts[(ts.index >= pre_start) & (ts.index <= post_end)]
        if len(ts) < 14:
            return None
        rows = []
        for d in ts.index:
            for site in ALL_SITES:
                rows.append({
                    'date': d, 'site': site,
                    'visits': ts.loc[d, site] if site in ts.columns else 0,
                    'treat': 1 if site in treated else 0,
                    'post': 1 if d >= bdate else 0,
                })
        panel = pd.DataFrame(rows)
        panel['treat_post'] = panel['treat'] * panel['post']
        panel = panel.merge(conf.reset_index(), on='date', how='left')
        y = panel['visits']
        x_cols = ['treat', 'post', 'treat_post'] + [c for c in cov_cols if c in panel.columns]
        X = sm.add_constant(panel[x_cols].fillna(0))
        try:
            model = sm.OLS(y, X).fit(cov_type='HC1')
            dc = float(model.params.get('treat_post', 0))
            dp = float(model.pvalues.get('treat_post', 1))
            ci = model.conf_int().loc['treat_post']
            tb = float(panel[(panel['treat'] == 1) & (panel['post'] == 0)]['visits'].mean())
            return {
                'method': 'Covariate DID', 'name': name,
                'did_effect': dc, 'did_pvalue': dp,
                'ci_lower': float(ci[0]), 'ci_upper': float(ci[1]),
                'r_squared': float(model.rsquared), 'n_obs': int(model.nobs),
                'significant': dp < 0.1, 'treat_baseline': tb,
                'effect_pct': dc / tb if tb else 0,
            }
        except Exception as e:
            print(f"  CovDID {name}: ERROR - {e}"); return None

# ── 실행 ──
print("\n=== 분석 시작 ===\n")
results = {}
for name, info in BROADCASTS.items():
    if name == '황제파워':
        print(f"[{name}] 제외"); continue
    if name == '전국노래자랑':
        print(f"[{name}] T맵 불가, 온라인 버즈만")
        results[name] = {'ci': None, 'yoy': None, 'cov': None}; continue

    print(f"\n[{name}]")
    ci_r = run_causal_impact(name, info) if info['sites'] != 'all' else None
    if info['sites'] == 'all':
        print(f"  CI {name}: 불가 (아산 전체)")
    yoy_r = run_yoy_did(name, info)
    cov_r = run_covariate_did(name, info)
    if cov_r:
        m = cov_r['method']
        eff = cov_r.get('effect_pct', 0)
        pv = cov_r.get('did_pvalue', cov_r.get('post_pvalue', 1))
        print(f"  {m} {name}: effect={eff:.1%}, p={pv:.3f}")
    results[name] = {'ci': ci_r, 'yoy': yoy_r, 'cov': cov_r}

# ── 종합 차트 ──
broadcast_names = [n for n in BROADCASTS if n != '황제파워']

# 차트 1: 방법론별 효과 비교
fig, ax = plt.subplots(figsize=(12, 6))
x = np.arange(len(broadcast_names))
width = 0.25
method_labels = ['CausalImpact', 'YoY DID', 'Cov DID/Pre-Post']
colors = ['#2196F3', '#FF9800', '#4CAF50']

for i, ml in enumerate(method_labels):
    vals = []
    for name in broadcast_names:
        r = results.get(name, {})
        if ml == 'CausalImpact' and r.get('ci'):
            vals.append(r['ci']['avg_effect_rel'] * 100)
        elif ml == 'YoY DID' and r.get('yoy'):
            vals.append(r['yoy']['did_effect'] * 100)
        elif ml == 'Cov DID/Pre-Post' and r.get('cov'):
            vals.append(r['cov'].get('effect_pct', 0) * 100)
        else:
            vals.append(0)
    bars = ax.bar(x + i * width, vals, width, label=ml, color=colors[i], alpha=0.85, edgecolor='#333', lw=0.5)
    for j, (v, b) in enumerate(zip(vals, bars)):
        if v != 0:
            ax.text(b.get_x() + b.get_width()/2, v + (1 if v >= 0 else -2),
                   f'{v:.1f}%', ha='center', va='bottom' if v >= 0 else 'top', fontsize=7)

ax.set_xticks(x + width); ax.set_xticklabels(broadcast_names, rotation=15, ha='right', fontsize=9)
ax.axhline(0, color='gray', lw=0.8)
ax.set_ylabel('추정 효과 (%)'); ax.set_title('방송별 홍보효과 비교: 3가지 방법론', fontsize=14, fontweight='bold')
ax.legend(fontsize=9); ax.grid(axis='y', alpha=0.3)
plt.tight_layout(); fig.savefig(OUT / 'comparison_methods.png', dpi=150, bbox_inches='tight'); plt.close(fig)
print("\n차트1 저장")

# 차트 2: 비용효율
fig, ax = plt.subplots(figsize=(10, 6))
ns, cs, es = [], [], []
for name in broadcast_names:
    if name == '전국노래자랑': continue
    r = results.get(name, {})
    eff = None
    if r.get('ci'): eff = r['ci']['avg_effect_rel'] * 100
    elif r.get('yoy'): eff = r['yoy']['did_effect'] * 100
    elif r.get('cov'): eff = r['cov'].get('effect_pct', 0) * 100
    if eff is not None:
        ns.append(name); cs.append(BROADCASTS[name]['budget'] / 1000); es.append(eff)

clrs = ['#E53935' if e < 0 else '#43A047' for e in es]
ax.scatter(cs, es, s=150, c=clrs, edgecolors='#333', lw=1, zorder=5)
for i, n in enumerate(ns):
    ax.annotate(n, (cs[i], es[i]), textcoords="offset points", xytext=(8, 8), fontsize=9)
ax.axhline(0, color='gray', lw=0.8, ls='--')
ax.set_xlabel('예산 (백만원)'); ax.set_ylabel('추정 효과 (%)')
ax.set_title('비용 대비 홍보효과', fontsize=14, fontweight='bold')
ax.grid(alpha=0.3); plt.tight_layout()
fig.savefig(OUT / 'cost_efficiency.png', dpi=150, bbox_inches='tight'); plt.close(fig)
print("차트2 저장")

# 차트 3: 히트맵
fig, ax = plt.subplots(figsize=(10, 5))
mnames = [n for n in broadcast_names if n != '전국노래자랑']
matrix = np.full((len(mnames), 3), np.nan)
for i, name in enumerate(mnames):
    r = results.get(name, {})
    if r.get('ci'): matrix[i, 0] = r['ci']['avg_effect_rel'] * 100
    if r.get('yoy'): matrix[i, 1] = r['yoy']['did_effect'] * 100
    if r.get('cov'): matrix[i, 2] = r['cov'].get('effect_pct', 0) * 100

vals = matrix[~np.isnan(matrix)]
vmin, vmax = (vals.min() if len(vals) else -1), (vals.max() if len(vals) else 1)
if vmin >= 0: vmin = -1
if vmax <= 0: vmax = 1
norm = TwoSlopeNorm(vmin=vmin, vcenter=0, vmax=vmax)
im = ax.imshow(matrix, cmap='RdYlGn', norm=norm, aspect='auto')
ax.set_xticks(range(3)); ax.set_xticklabels(['CausalImpact', 'YoY DID', '공변량 DID/Pre-Post'], fontsize=10)
ax.set_yticks(range(len(mnames))); ax.set_yticklabels(mnames, fontsize=10)
ax.set_title('방법론별 효과 추정 (%, 양수=긍정)', fontsize=13, fontweight='bold')
for i in range(len(mnames)):
    for j in range(3):
        v = matrix[i, j]
        txt = 'N/A' if np.isnan(v) else f'{v:.1f}%'
        clr = 'gray' if np.isnan(v) else 'black'
        ax.text(j, i, txt, ha='center', va='center', fontsize=10, fontweight='bold', color=clr)
plt.colorbar(im, ax=ax, label='효과 (%)'); plt.tight_layout()
fig.savefig(OUT / 'result_heatmap.png', dpi=150, bbox_inches='tight'); plt.close(fig)
print("차트3 저장")

# ── PDF 생성 ──
print("\n=== PDF 리포트 생성 ===")

def img_b64(path):
    with open(path, 'rb') as f:
        return base64.b64encode(f.read()).decode()

def broadcast_html(name, info, res):
    bdate = info['date']
    channel = info['channel']
    rating = info['rating']
    budget = info['budget']
    sites = info['sites'] if info['sites'] != 'all' else '아산 전체'
    if isinstance(sites, list): sites = ', '.join(sites)

    h = f"""<div class="bsec">
    <h3>{name}</h3>
    <table class="it"><tr><td>방영일</td><td>{bdate}</td><td>채널</td><td>{channel}</td></tr>
    <tr><td>시청률</td><td>{rating}%</td><td>예산</td><td>{budget:,}천원</td></tr>
    <tr><td colspan="4">노출: {sites}</td></tr></table>"""

    if name == '전국노래자랑':
        h += """<p><b>T맵 분석:</b> 2025-05~10월 데이터 부재로 방문 데이터 기반 분석 불가.</p>
        <p><b>온라인 버즈:</b> 네이버 블로그 '전국노래자랑 아산시편' 149건, 뉴스 94건.
        YouTube 사전 37편(94만 조회) 대비 사후 51편(268만 조회)으로 조회수 185% 증가.</p>
        <p class="interp"><b>해석:</b> 온라인 콘텐츠 확산에는 효과적. KBS1 시청률 6.5%의 높은 노출도.
        최소 비용(3,518천원)으로 진행되어 비용효율 우수. 다만 관광 유발 효과 직접 검증 불가.</p></div>"""
        return h

    ci = res.get('ci')
    if ci:
        sig = "유의 (신뢰구간이 0 미포함)" if ci['significant'] else "비유의 (신뢰구간이 0 포함)"
        h += f"""<h4>방법 1: Bayesian CausalImpact</h4>
        <table class="rt"><tr><th>지표</th><th>값</th></tr>
        <tr><td>사후 실제 평균</td><td>{ci['avg_actual']:.1f}명/일</td></tr>
        <tr><td>반사실 예측 평균</td><td>{ci['avg_predicted']:.1f}명/일</td></tr>
        <tr><td>평균 인과효과</td><td>{ci['avg_effect_abs']:.1f}명/일 ({ci['avg_effect_rel']:.1%})</td></tr>
        <tr><td>누적 인과효과</td><td>{ci['cum_effect']:.0f}명 ({ci['cum_effect_rel']:.1%})</td></tr>
        <tr><td>누적 95% CI</td><td>[{ci['cum_ci_lower']:.0f}, {ci['cum_ci_upper']:.0f}]</td></tr>
        <tr><td>유의성</td><td>{sig}</td></tr></table>
        <img src="data:image/png;base64,{img_b64(ci['chart'])}" class="ci"/>"""
    elif info['sites'] == 'all':
        h += "<p><em>CausalImpact: 아산 전체 대상, 대조군 설정 불가로 미적용.</em></p>"
    else:
        h += "<p><em>CausalImpact: 적용 불가.</em></p>"

    yoy = res.get('yoy')
    if yoy:
        h += f"""<h4>방법 2: 전년 동기 DID (2025 Q1 vs 2026 Q1)</h4>
        <table class="rt"><tr><th></th><th>처치군</th><th>대조군</th></tr>
        <tr><td>2025 Q1 방문</td><td>{yoy['treat_prev']:,}명</td><td>{yoy['ctrl_prev']:,}명</td></tr>
        <tr><td>2026 Q1 방문</td><td>{yoy['treat_curr']:,}명</td><td>{yoy['ctrl_curr']:,}명</td></tr>
        <tr><td>YoY 변화율</td><td>{yoy['treat_change_pct']:.1%}</td><td>{yoy['ctrl_change_pct']:.1%}</td></tr></table>
        <p><b>DID = {yoy['did_effect']:.1%}p</b> (처치군 성장률 - 대조군 성장률)</p>
        <p class="interp">처치군이 대조군 대비 {abs(yoy['did_effect']):.1%}p {'더 성장' if yoy['did_effect'] > 0 else '덜 성장'}. 
        전년 동일 시기 비교로 계절 효과가 자연 통제됨.</p>"""
    else:
        if name == '뛰어야산다2':
            h += "<p><em>전년 동기 DID: 계산 실패.</em></p>"
        else:
            h += "<p><em>전년 동기 DID: 전년 동기 데이터 매칭 불가, 미적용.</em></p>"

    cov = res.get('cov')
    if cov:
        mn = cov['method']
        if 'did_effect' in cov:
            ev, ep, pv = cov['did_effect'], cov['effect_pct'], cov['did_pvalue']
            cl, cu = cov['ci_lower'], cov['ci_upper']
        else:
            ev, ep, pv = cov['post_effect'], cov['effect_pct'], cov['post_pvalue']
            cl, cu = cov['ci_lower'], cov['ci_upper']
        sig = "유의 (p<0.1)" if cov['significant'] else "비유의 (p>=0.1)"
        h += f"""<h4>방법 3: {mn} (날씨/시즌 통제)</h4>
        <table class="rt"><tr><th>지표</th><th>값</th></tr>
        <tr><td>추정 효과</td><td>{ev:.1f}명/일 ({ep:.1%})</td></tr>
        <tr><td>95% CI</td><td>[{cl:.1f}, {cu:.1f}]</td></tr>
        <tr><td>p-value</td><td>{pv:.4f}</td></tr>
        <tr><td>유의성</td><td>{sig}</td></tr>
        <tr><td>R-squared</td><td>{cov['r_squared']:.3f}</td></tr>
        <tr><td>관측수</td><td>{cov['n_obs']}</td></tr></table>
        <p>통제 변수: 기온, 강수량, 주말, 공휴일, 시즌점수</p>"""

    # 종합 해석
    parts = []
    if ci:
        d = "양(+)" if ci['avg_effect_rel'] > 0 else "음(-)"
        parts.append(f"CausalImpact: {d} {ci['avg_effect_rel']:.1%}, {'유의' if ci['significant'] else '비유의'}")
    if yoy:
        d = "양(+)" if yoy['did_effect'] > 0 else "음(-)"
        parts.append(f"전년동기DID: {d} {yoy['did_effect']:.1%}p")
    if cov:
        d = "양(+)" if ep > 0 else "음(-)"
        parts.append(f"공변량모델: {d} {ep:.1%}, p={pv:.3f}")

    if parts:
        h += f'<p class="interp"><b>종합:</b> {". ".join(parts)}.</p>'
    h += "</div>"
    return h

# 종합표
def summary_table_html():
    h = """<table class="st">
    <tr><th rowspan="2">방송</th><th colspan="2">CausalImpact</th><th colspan="2">전년동기 DID</th>
    <th colspan="2">공변량 DID/Pre-Post</th><th rowspan="2">예산(천원)</th><th rowspan="2">일관성</th></tr>
    <tr><th>효과(%)</th><th>유의</th><th>효과(%p)</th><th>유의</th><th>효과(%)</th><th>유의</th></tr>"""
    for name in BROADCASTS:
        if name == '황제파워': continue
        info = BROADCASTS[name]; r = results.get(name, {})
        ci = r.get('ci'); yoy = r.get('yoy'); cov = r.get('cov')
        ce = f"{ci['avg_effect_rel']*100:.1f}" if ci else '-'
        cs = 'O' if ci and ci['significant'] else ('X' if ci else '-')
        ye = f"{yoy['did_effect']*100:.1f}" if yoy else '-'
        ys = 'O' if yoy and yoy['significant'] else ('X' if yoy else '-')
        ve = f"{cov.get('effect_pct',0)*100:.1f}" if cov else '-'
        vs = 'O' if cov and cov['significant'] else ('X' if cov else '-')
        dirs = []
        if ci: dirs.append(ci['avg_effect_rel'] > 0)
        if yoy: dirs.append(yoy['did_effect'] > 0)
        if cov: dirs.append(cov.get('effect_pct', 0) > 0)
        con = '일치' if len(dirs) >= 2 and len(set(dirs)) == 1 else ('불일치' if len(dirs) >= 2 else ('단일' if dirs else '-'))
        h += f"<tr><td>{name}</td><td>{ce}</td><td>{cs}</td><td>{ye}</td><td>{ys}</td><td>{ve}</td><td>{vs}</td><td>{info['budget']:,}</td><td>{con}</td></tr>"
    h += "</table>"
    return h

bsections = "".join(broadcast_html(n, BROADCASTS[n], results.get(n, {})) for n in BROADCASTS if n != '황제파워')
stbl = summary_table_html()
cm_b64 = img_b64(OUT / 'comparison_methods.png')
ce_b64 = img_b64(OUT / 'cost_efficiency.png')
hm_b64 = img_b64(OUT / 'result_heatmap.png')

# 결론
conclusion_items = []
for name in ['뛰어야산다2', '같이삽시다3', '굿모닝대한민국', '전현무계획2', '6시내고향', '전국노래자랑']:
    r = results.get(name, {})
    ci = r.get('ci'); yoy = r.get('yoy'); cov = r.get('cov')
    pos, tot = 0, 0
    if ci: tot += 1; pos += (ci['avg_effect_rel'] > 0)
    if yoy: tot += 1; pos += (yoy['did_effect'] > 0)
    if cov: tot += 1; pos += (cov.get('effect_pct', 0) > 0)
    b = BROADCASTS[name]['budget']
    if name == '전국노래자랑':
        conclusion_items.append(f"<li><b>{name}:</b> T맵 분석 불가. YouTube 조회수 185% 증가 등 온라인 버즈 확인. 최소 비용({b:,}천원)으로 높은 비용효율.</li>")
    elif tot == 0:
        conclusion_items.append(f"<li><b>{name}:</b> 분석 불가.</li>")
    elif pos == tot:
        conclusion_items.append(f"<li><b>{name}:</b> {tot}가지 방법 모두 양(+)의 효과. 방송의 관광 유발 효과가 일관되게 확인됨. 예산 {b:,}천원.</li>")
    elif pos == 0:
        conclusion_items.append(f"<li><b>{name}:</b> {tot}가지 방법 모두 음(-)의 효과. 계절 효과를 통제하더라도 순수 방문 유발 효과 제한적. 예산 {b:,}천원.</li>")
    else:
        conclusion_items.append(f"<li><b>{name}:</b> {tot}가지 방법 중 {pos}개 양(+), 결과 혼재. 예산 {b:,}천원.</li>")

html = f"""<!DOCTYPE html><html lang="ko"><head><meta charset="UTF-8">
<style>
@page {{ size: A4; margin: 15mm; }}
body {{ font-family: 'AppleGothic', sans-serif; font-size: 10pt; line-height: 1.5; color: #222; }}
h1 {{ font-size: 18pt; text-align: center; margin-bottom: 5px; }}
h2 {{ font-size: 14pt; border-bottom: 2px solid #333; padding-bottom: 4px; margin-top: 20px; }}
h3 {{ font-size: 12pt; border-left: 4px solid #2196F3; padding-left: 8px; margin-top: 15px; }}
h4 {{ font-size: 10pt; color: #555; margin-top: 10px; }}
.sub {{ text-align: center; color: #666; font-size: 10pt; margin-bottom: 20px; }}
table {{ border-collapse: collapse; width: 100%; margin: 8px 0; }}
th, td {{ border: 1px solid #ccc; padding: 4px 8px; text-align: center; font-size: 9pt; }}
th {{ background: #f5f5f5; font-weight: bold; }}
.it td:first-child, .it td:nth-child(3) {{ font-weight: bold; background: #fafafa; width: 80px; }}
.rt th {{ width: 140px; text-align: left; }}
.rt td:first-child {{ text-align: left; font-weight: bold; }}
.st th {{ font-size: 8pt; }}
.ci {{ width: 100%; max-width: 700px; display: block; margin: 10px auto; }}
.bsec {{ margin-bottom: 20px; page-break-inside: avoid; }}
.interp {{ background: #f9f9f9; border: 1px solid #ddd; padding: 8px; margin: 8px 0; font-size: 9pt; }}
.mb {{ border: 1px solid #999; padding: 10px; margin: 10px 0; }}
.mb h4 {{ margin-top: 0; }}
.note {{ color: #888; font-size: 8pt; }}
</style></head><body>
<h1>아산시 방송 홍보효과 비교분석</h1>
<p class="sub">3가지 방법론: CausalImpact / 전년 동기 DID / 공변량 DID<br/>
분석일: 2026-05-13 | 데이터: T맵 내비게이션 (2025.01~2026.03)</p>

<h2>1. 분석 개요</h2>
<p>본 보고서는 아산시가 2025~2026년 집행한 7개 방송 프로그램의 관광 홍보효과를
T맵 내비게이션 방문 데이터로 분석한다. 단순 전후비교(naive DID)는 가을에서 겨울로의
비수기 진입에 따른 계절 효과를 분리하지 못하는 한계가 있어, 3가지 인과추론 방법론을 적용하였다.</p>

<div class="mb"><h4>방법 1: Bayesian Structural Time Series (CausalImpact)</h4>
<p>처치군(방송 노출 관광지)의 사후 방문수를 대조군(미노출 관광지) 시계열로 예측한
반사실(counterfactual)과 비교. 요일 계절성(n_seasons=7) 포함.</p>
<p class="note">적용: 특정 관광지 노출 3개 방송 (굿모닝대한민국, 같이삽시다3, 뛰어야산다2)</p></div>

<div class="mb"><h4>방법 2: 전년 동기 이중차분법 (YoY DID)</h4>
<p>동일 월 전년(2025) vs 금년(2026) 비교로 계절 효과 자연 통제.
처치군/대조군 각각의 전년 대비 변화율 차이가 DID 추정치.</p>
<p class="note">적용: 뛰어야산다2 (2026 Q1 vs 2025 Q1)</p></div>

<div class="mb"><h4>방법 3: 공변량 통제 DID / 전후비교</h4>
<p>기온, 강수량, 주말, 공휴일, 시즌점수를 공변량으로 포함하는 OLS 회귀.
HC1 robust SE 사용. 관광지 기반 처치/대조 가능 시 패널 DID, 아산 전체 대상 시 전후비교.</p>
<p class="note">적용: T맵 데이터 있는 5개 방송 전체</p></div>

<h2>2. 분석 대상</h2>
<table><tr><th>방송</th><th>방영일</th><th>채널</th><th>시청률</th><th>예산(천원)</th><th>노출</th><th>적용 방법</th></tr>
<tr><td>전국노래자랑</td><td>2025-06-08</td><td>KBS1</td><td>6.5%</td><td>3,518</td><td>신정호</td><td>온라인 버즈</td></tr>
<tr><td>전현무계획2</td><td>2025-11-07</td><td>MBN</td><td>1.5%</td><td>50,000</td><td>아산 전체</td><td>Cov Pre-Post</td></tr>
<tr><td>굿모닝대한민국</td><td>2025-11-12</td><td>KBS2</td><td>0.55%</td><td>20,000</td><td>온양온천 등 4곳</td><td>CI + CovDID</td></tr>
<tr><td>6시내고향</td><td>2025-11-13</td><td>KBS1</td><td>5.5%</td><td>110,000</td><td>아산 전체</td><td>Cov Pre-Post</td></tr>
<tr><td>같이삽시다3</td><td>2025-11-24~12-15</td><td>KBS2</td><td>3.0%</td><td>133,000</td><td>곡교천 등 5곳</td><td>CI + CovDID</td></tr>
<tr><td>뛰어야산다2</td><td>2026-01-12</td><td>MBN</td><td>1.5%</td><td>45,000</td><td>신정호 등 4곳</td><td>CI + YoY + CovDID</td></tr>
<tr><td>황제파워</td><td>2026-05-09</td><td>SBS FM</td><td>-</td><td>220,000</td><td>온양온천</td><td>제외(사후 데이터 없음)</td></tr></table>

<h2>3. 방송별 상세 결과</h2>
{bsections}

<h2>4. 종합 비교</h2>
<h3>4.1 방법론별 효과 종합표</h3>
{stbl}

<h3>4.2 효과 비교 시각화</h3>
<img src="data:image/png;base64,{cm_b64}" class="ci"/>

<h3>4.3 결과 일관성 히트맵</h3>
<img src="data:image/png;base64,{hm_b64}" class="ci"/>

<h3>4.4 비용 대비 효과</h3>
<img src="data:image/png;base64,{ce_b64}" class="ci"/>

<h2>5. 교란요소 및 한계</h2>
<table><tr><th>방송</th><th>교란요소</th><th>기온차</th><th>날씨양호일 변화</th></tr>
<tr><td>전현무계획2</td><td>없음</td><td>-7.1도</td><td>-4일</td></tr>
<tr><td>굿모닝대한민국</td><td>가을단풍시즌</td><td>-6.7도</td><td>-8일</td></tr>
<tr><td>6시내고향</td><td>없음</td><td>-6.4도</td><td>-8일</td></tr>
<tr><td>같이삽시다3</td><td>단풍+온천시즌</td><td>-5.4도</td><td>-4일</td></tr>
<tr><td>뛰어야산다2</td><td>비수기(1월)</td><td>-2.6도</td><td>0일</td></tr></table>

<p>2025년 11월 방송 4건은 가을→겨울 전환기에 방영되어 기온 5~7도 하락, 날씨 양호일 4~8일 감소의
강한 계절 교란이 존재한다. 이로 인해 naive DID에서는 모든 방송이 음수 효과를 보인다.
본 분석은 이 계절 효과를 3가지 방법으로 통제하여 순수 방송 효과를 추정하였다.</p>

<p><b>데이터 한계:</b></p>
<ul>
<li>2025년 5~10월 T맵 부재로 전국노래자랑(6월) 분석 불가</li>
<li>전현무계획2, 6시내고향은 "아산 전체" 대상으로 관광지 기반 처치/대조군 설정 불가</li>
<li>전년 동기 DID는 뛰어야산다2에만 깔끔하게 적용 가능</li>
<li>2025-11월 방송 4건이 1~2주 간격으로 연속 방영되어 개별 효과 분리 어려움</li>
</ul>

<h2>6. 결론 및 정책 제언</h2>
<ul>{"".join(conclusion_items)}</ul>

<p><b>정책 제언:</b></p>
<ol>
<li><b>비수기 방송의 효과 해석에 주의.</b> 가을→겨울 전환기(11월) 집중 방송은 계절 효과와 혼재.
방송 시기를 분산하면 효과 측정과 실질 효과 극대화 모두에 유리.</li>
<li><b>특정 관광지 노출 방송이 분석에 유리.</b> "아산 전체" 방송은 엄밀한 인과추론 적용 불가.
특정 관광지 노출이 효과 측정과 타겟 마케팅에 유리.</li>
<li><b>비용효율 관점에서 소규모/고시청률 방송 유리.</b> 전국노래자랑은 최소 비용으로 높은 시청률,
뛰어야산다2는 중간 예산으로 가장 일관된 양의 효과.</li>
<li><b>연속 방영 간격 확보 필요.</b> 11월 4개 방송이 1~2주 간격으로 방영되어 개별 효과 분리 불가.
향후 최소 4주 이상 간격 권장.</li>
</ol>
</body></html>"""

from weasyprint import HTML
pdf_path = '/Users/eomgyuhyeon/.openclaw/workspace/아산시_방송홍보효과_비교분석.pdf'
HTML(string=html).write_pdf(pdf_path)
print(f"\nPDF 완료: {pdf_path}")
print("=== 분석 완료 ===")
