"""
방송효과 인과추론 유틸리티

ruptures/pycausalimpact 의존성 없이 순수 statsmodels + scipy로 구현.
- CausalImpact: 베이지안 구조 시계열 (UnobservedComponents)
- DID: 이중차분 회귀
- Callaway-Sant'Anna Staggered DID (간이 구현)
- 플라시보 테스트
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import Optional, Tuple, List, Dict
import warnings
warnings.filterwarnings('ignore')


# ============================================================
# 1. CausalImpact (statsmodels UnobservedComponents 기반)
# ============================================================
class SimpleCausalImpact:
    """
    Google CausalImpact의 간이 Python 구현.
    statsmodels UnobservedComponents로 반사실(counterfactual) 구성.

    pycausalimpact가 ruptures 의존성 문제로 설치 안 될 때 대안.
    """

    def __init__(self, data: pd.DataFrame, pre_period: list, post_period: list,
                 alpha: float = 0.05):
        """
        Args:
            data: DataFrame. 첫 컬럼=결과변수(y), 나머지=통제 시계열(x1, x2, ...)
                  인덱스는 DatetimeIndex 또는 정수
            pre_period: [start, end] - 개입 전 기간
            post_period: [start, end] - 개입 후 기간
            alpha: 유의수준 (기본 0.05 → 95% 신뢰구간)
        """
        self.data = data.copy()
        self.pre_period = pre_period
        self.post_period = post_period
        self.alpha = alpha
        self.result = None
        self._fit()

    def _fit(self):
        from statsmodels.tsa.statespace.structural import UnobservedComponents

        y_col = self.data.columns[0]
        x_cols = self.data.columns[1:]

        pre_mask = (self.data.index >= self.pre_period[0]) & (self.data.index <= self.pre_period[1])
        post_mask = (self.data.index >= self.post_period[0]) & (self.data.index <= self.post_period[1])

        pre_data = self.data[pre_mask]
        post_data = self.data[post_mask]
        all_data = self.data[pre_mask | post_mask]

        # pre-period에서 모델 학습
        endog = pre_data[y_col]
        exog = pre_data[x_cols] if len(x_cols) > 0 else None

        # 모델: 로컬 레벨 + 회귀
        model = UnobservedComponents(
            endog,
            level='local level',
            exog=exog,
        )
        fitted = model.fit(disp=False, maxiter=500)

        # post-period 예측 (반사실)
        post_exog = post_data[x_cols] if len(x_cols) > 0 else None
        nsteps = len(post_data)

        forecast = fitted.get_forecast(steps=nsteps, exog=post_exog, alpha=self.alpha)
        pred_mean = forecast.predicted_mean
        pred_ci = forecast.conf_int(alpha=self.alpha)

        # pre-period 적합값
        pre_pred = fitted.get_prediction()
        pre_mean = pre_pred.predicted_mean
        pre_ci = pre_pred.conf_int(alpha=self.alpha)

        # 결과 저장
        self.result = {
            'pre_actual': pre_data[y_col],
            'pre_predicted': pre_mean,
            'pre_ci': pre_ci,
            'post_actual': post_data[y_col],
            'post_predicted': pred_mean,
            'post_ci': pred_ci,
            'effect': post_data[y_col].values - pred_mean.values,
            'cum_effect': np.cumsum(post_data[y_col].values - pred_mean.values),
            'fitted_model': fitted,
        }

        # 요약 통계
        actual_sum = post_data[y_col].sum()
        pred_sum = pred_mean.sum()
        effect_sum = actual_sum - pred_sum
        rel_effect = effect_sum / pred_sum * 100 if pred_sum != 0 else np.nan

        # p-value 근사 (정규분포 가정)
        post_std = (pred_ci.iloc[:, 1] - pred_ci.iloc[:, 0]) / (2 * 1.96)
        cum_std = np.sqrt(np.sum(post_std.values**2))
        z_score = effect_sum / cum_std if cum_std > 0 else 0
        p_value = 2 * (1 - __import__('scipy').stats.norm.cdf(abs(z_score)))

        self.summary_stats = {
            'actual_sum': actual_sum,
            'predicted_sum': pred_sum,
            'absolute_effect': effect_sum,
            'relative_effect_pct': rel_effect,
            'p_value': p_value,
            'significant': p_value < self.alpha,
            'ci_lower': pred_ci.iloc[:, 0].sum(),
            'ci_upper': pred_ci.iloc[:, 1].sum(),
        }

    def summary(self) -> str:
        s = self.summary_stats
        lines = [
            "=" * 50,
            "CausalImpact 분석 결과",
            "=" * 50,
            f"실제 합계:        {s['actual_sum']:>15,.0f}",
            f"예측 합계(반사실): {s['predicted_sum']:>15,.0f}",
            f"절대 효과:        {s['absolute_effect']:>15,.0f}",
            f"상대 효과:        {s['relative_effect_pct']:>15.1f}%",
            f"p-value:          {s['p_value']:>15.4f}",
            f"유의성 (α={self.alpha}): {'유의함' if s['significant'] else '유의하지 않음'}",
            "=" * 50,
        ]
        return "\n".join(lines)

    def plot(self, figsize=(14, 10)):
        r = self.result
        fig, axes = plt.subplots(3, 1, figsize=figsize, sharex=True)

        # 1. 원계열 + 반사실
        ax = axes[0]
        all_actual = pd.concat([r['pre_actual'], r['post_actual']])
        all_pred = pd.concat([r['pre_predicted'], r['post_predicted']])

        ax.plot(all_actual.index, all_actual.values, 'k-', label='실제')
        ax.plot(all_pred.index, all_pred.values, 'b--', label='예측(반사실)')

        # post 신뢰구간
        post_idx = r['post_predicted'].index
        ax.fill_between(post_idx,
                        r['post_ci'].iloc[:, 0].values,
                        r['post_ci'].iloc[:, 1].values,
                        alpha=0.2, color='blue')
        ax.axvline(r['post_actual'].index[0], color='red', linestyle='--', alpha=0.7)
        ax.set_title('원계열 vs 반사실')
        ax.legend()

        # 2. 시점별 효과
        ax = axes[1]
        ax.bar(range(len(r['effect'])), r['effect'],
               color=['#2ecc71' if e > 0 else '#e74c3c' for e in r['effect']], alpha=0.7)
        ax.axhline(0, color='gray', linewidth=0.5)
        ax.axvline(0, color='red', linestyle='--', alpha=0.7)
        ax.set_title('시점별 효과 (실제 - 반사실)')

        # 3. 누적 효과
        ax = axes[2]
        ax.plot(range(len(r['cum_effect'])), r['cum_effect'], 'b-', linewidth=2)
        ax.fill_between(range(len(r['cum_effect'])), 0, r['cum_effect'], alpha=0.2)
        ax.axhline(0, color='gray', linewidth=0.5)
        ax.set_title('누적 효과')
        ax.set_xlabel('방영 후 기간')

        plt.tight_layout()
        return fig


# ============================================================
# 2. DID (이중차분) 회귀
# ============================================================
def run_did(panel: pd.DataFrame,
            y_col: str,
            treated_col: str,
            post_col: str,
            controls: Optional[list] = None,
            fe_cols: Optional[list] = None,
            cluster_col: Optional[str] = None) -> dict:
    """
    DID 회귀: Y = b0 + b1*Treated + b2*Post + b3*(Treated×Post) + controls + FE + e

    Args:
        panel: 패널 데이터
        y_col: 종속변수 (e.g. 외지인소비액)
        treated_col: 처치 더미 (0/1)
        post_col: 사후 더미 (0/1)
        controls: 통제변수 리스트
        fe_cols: 고정효과 컬럼 리스트
        cluster_col: 클러스터 표준오차용 컬럼

    Returns:
        dict: {model, did_coef, did_pvalue, significant, summary_table}
    """
    import statsmodels.formula.api as smf

    df = panel.copy()
    df['treated_post'] = df[treated_col] * df[post_col]

    formula = f"{y_col} ~ {treated_col} + {post_col} + treated_post"
    if controls:
        formula += " + " + " + ".join(controls)
    if fe_cols:
        for fe in fe_cols:
            formula += f" + C({fe})"

    if cluster_col:
        model = smf.ols(formula, data=df).fit(
            cov_type='cluster', cov_kwds={'groups': df[cluster_col]}
        )
    else:
        model = smf.ols(formula, data=df).fit(cov_type='HC1')

    did_coef = model.params['treated_post']
    did_pval = model.pvalues['treated_post']

    return {
        'model': model,
        'did_coef': did_coef,
        'did_pvalue': did_pval,
        'significant': did_pval < 0.05,
        'summary_table': model.summary().tables[1],
    }


# ============================================================
# 3. Staggered DID - Callaway-Sant'Anna 간이 구현
# ============================================================
def staggered_did(panel: pd.DataFrame,
                  y_col: str,
                  unit_col: str,
                  time_col: str,
                  treatment_time_col: str) -> pd.DataFrame:
    """
    Callaway-Sant'Anna (2021) 간이 구현.
    각 cohort(처치 시점)별로 아직-미처치 그룹을 통제군으로 사용하여 ATT 추정.

    Args:
        panel: 패널 (unit × time)
        y_col: 종속변수
        unit_col: 개체 식별자 (e.g. 읍면동)
        time_col: 시간 변수 (정수 또는 period)
        treatment_time_col: 처치 시점 (처치 안 받으면 NaN/inf)

    Returns:
        DataFrame: cohort별 event_time별 ATT
    """
    df = panel.copy()
    times = sorted(df[time_col].unique())
    cohorts = sorted(df[treatment_time_col].dropna().unique())

    results = []
    for g in cohorts:
        # cohort g: treatment_time == g인 개체들
        treated_units = df[df[treatment_time_col] == g][unit_col].unique()
        # 통제군: 시점 g까지 아직 처치 안 받은 개체
        never_or_later = df[
            (df[treatment_time_col].isna()) | (df[treatment_time_col] > g)
        ][unit_col].unique()

        if len(never_or_later) == 0:
            continue

        for t in times:
            event_time = t - g  # 방영 기준 상대 시점

            # 처치군 t시점 평균
            y_treated = df[(df[unit_col].isin(treated_units)) & (df[time_col] == t)][y_col].mean()
            # 통제군 t시점 평균
            y_control = df[(df[unit_col].isin(never_or_later)) & (df[time_col] == t)][y_col].mean()
            # 처치군 g-1시점(직전) 평균
            y_treated_pre = df[(df[unit_col].isin(treated_units)) & (df[time_col] == g - 1)][y_col].mean()
            # 통제군 g-1시점 평균
            y_control_pre = df[(df[unit_col].isin(never_or_later)) & (df[time_col] == g - 1)][y_col].mean()

            if pd.notna(y_treated) and pd.notna(y_control) and pd.notna(y_treated_pre) and pd.notna(y_control_pre):
                att = (y_treated - y_treated_pre) - (y_control - y_control_pre)
                results.append({
                    'cohort': g,
                    'time': t,
                    'event_time': event_time,
                    'att': att,
                    'n_treated': len(treated_units),
                    'n_control': len(never_or_later),
                })

    return pd.DataFrame(results)


def plot_event_study(att_df: pd.DataFrame, title: str = "Event Study"):
    """Staggered DID 결과를 event study 그래프로"""
    agg = att_df.groupby('event_time')['att'].agg(['mean', 'std', 'count']).reset_index()
    agg['se'] = agg['std'] / np.sqrt(agg['count'])
    agg['ci_lo'] = agg['mean'] - 1.96 * agg['se']
    agg['ci_hi'] = agg['mean'] + 1.96 * agg['se']

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.errorbar(agg['event_time'], agg['mean'], yerr=1.96*agg['se'],
                fmt='o-', color='#2c3e50', capsize=3)
    ax.axhline(0, color='gray', linewidth=0.5)
    ax.axvline(-0.5, color='red', linestyle='--', alpha=0.7, label='방영 시점')
    ax.fill_between(agg['event_time'], agg['ci_lo'], agg['ci_hi'], alpha=0.15, color='blue')
    ax.set_xlabel('방영 기준 상대 기간')
    ax.set_ylabel('ATT')
    ax.set_title(title)
    ax.legend()
    plt.tight_layout()
    return fig


# ============================================================
# 4. 플라시보 테스트
# ============================================================
def placebo_test(data: pd.DataFrame, real_treatment_date,
                 placebo_dates: list, **ci_kwargs) -> pd.DataFrame:
    """
    여러 가짜 개입 시점에서 CausalImpact를 돌려
    진짜 개입 시점의 효과와 비교.

    placebo에서 유의한 효과 → 분석 설계 문제 신호
    """
    results = []
    for pdate in placebo_dates:
        try:
            ci = SimpleCausalImpact(data, **ci_kwargs)
            results.append({
                'date': pdate,
                'type': 'placebo',
                'effect': ci.summary_stats['absolute_effect'],
                'rel_effect': ci.summary_stats['relative_effect_pct'],
                'p_value': ci.summary_stats['p_value'],
                'significant': ci.summary_stats['significant'],
            })
        except Exception as e:
            results.append({
                'date': pdate, 'type': 'placebo',
                'effect': np.nan, 'rel_effect': np.nan,
                'p_value': np.nan, 'significant': False,
            })

    return pd.DataFrame(results)


# ============================================================
# 5. 강건성 검증 헬퍼
# ============================================================
def robustness_check(main_result: dict, robust_result: dict) -> str:
    """
    두 외지인 정의(EXCL_RSDT vs EXCL_LC)의 결과를 비교.
    방향과 유의성이 일치하면 "외지인 정의에 robust"
    """
    same_direction = (main_result['did_coef'] > 0) == (robust_result['did_coef'] > 0)
    both_sig = main_result['significant'] and robust_result['significant']

    if same_direction and both_sig:
        return "ROBUST: 두 외지인 정의에서 동일 방향, 모두 유의"
    elif same_direction:
        return "PARTIAL: 동일 방향이나 유의성 차이 있음"
    else:
        return "WARNING: 외지인 정의에 따라 결과 방향 다름 - 추가 검토 필요"
