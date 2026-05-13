"""
Step 8: 종합 리포트 생성 (PDF)
==============================
- Step 1~7 결과를 하나의 PDF 리포트로 합침
- 교란요소 통제 DID 결과
- 온라인 버즈 분석
- 종합 판정

실행: python step8_final_report.py
필요: pip install reportlab pandas numpy matplotlib
"""
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from pathlib import Path
from datetime import datetime

from config import BROADCASTS, OUTPUT_DIR

# 한글 폰트 설정
import platform
if platform.system() == "Darwin":
    plt.rcParams["font.family"] = "AppleGothic"
elif platform.system() == "Windows":
    plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False

REPORT_DIR = OUTPUT_DIR / "report"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 60)
print("Step 8: 종합 리포트 생성")
print("=" * 60)

# ============================================================
# 1. 데이터 로드
# ============================================================

def safe_load(filename, **kwargs):
    try:
        return pd.read_csv(OUTPUT_DIR / filename, encoding="utf-8-sig", **kwargs)
    except FileNotFoundError:
        print(f"  [!] {filename} 없음 (해당 step 미실행)")
        return pd.DataFrame()

df_buzz_summary = safe_load("online_buzz_summary.csv")
df_confounder_summary = safe_load("confounder_summary_by_broadcast.csv")
df_confounders = safe_load("confounders_merged.csv")
df_weather = safe_load("weather_daily.csv")
df_tmap_summary = safe_load("tmap_broadcast_effect_summary.csv")
df_did = safe_load("did_results.csv")
df_card_dong = safe_load("card_outsider_by_dong_monthly.csv")

# ============================================================
# 2. 시각화 생성
# ============================================================

print("\n--- 시각화 생성 ---")

def plot_weather_timeline(df_confounders, broadcasts, save_path):
    """날씨 + 방영일 타임라인"""
    if len(df_confounders) == 0 or "temperature_2m_mean" not in df_confounders.columns:
        return False

    df = df_confounders.copy()
    df["date"] = pd.to_datetime(df["date"])

    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)

    # 기온
    ax = axes[0]
    ax.fill_between(df["date"], df["temperature_2m_min"], df["temperature_2m_max"],
                    alpha=0.3, color="#3498db", label="기온 범위")
    ax.plot(df["date"], df["temperature_2m_mean"], color="#2c3e50", linewidth=1, label="평균기온")
    ax.set_ylabel("기온 (C)")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(True, alpha=0.3)

    # 강수/적설
    ax = axes[1]
    ax.bar(df["date"], df["precipitation_sum"], color="#3498db", alpha=0.7, label="강수(mm)")
    if "snowfall_sum" in df.columns:
        ax.bar(df["date"], df["snowfall_sum"], color="#95a5a6", alpha=0.7, label="적설(cm)")
    ax.set_ylabel("강수/적설")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(True, alpha=0.3)

    # 시즌 점수
    ax = axes[2]
    if "season_score" in df.columns:
        ax.fill_between(df["date"], df["season_score"], alpha=0.3, color="#e67e22")
        ax.plot(df["date"], df["season_score"], color="#e67e22", linewidth=1)
    ax.set_ylabel("관광시즌 점수")
    ax.set_ylim(0, 6)
    ax.grid(True, alpha=0.3)

    # 방영일 표시 (모든 subplot)
    colors = plt.cm.Set2(np.linspace(0, 1, len(broadcasts)))
    for i, bc in enumerate(broadcasts):
        air = pd.to_datetime(bc["air_date"])
        for ax in axes:
            ax.axvline(air, color=colors[i], linestyle="--", alpha=0.7, linewidth=1)
        axes[0].annotate(bc["name"][:6], xy=(air, axes[0].get_ylim()[1]),
                        fontsize=7, rotation=45, ha="left", va="bottom",
                        color=colors[i])

    fig.suptitle("아산시 날씨 & 방영일 타임라인", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()
    return True


def plot_buzz_comparison(df_buzz, save_path):
    """방송별 온라인 버즈 비교"""
    if len(df_buzz) == 0:
        return False

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # YouTube 영상 수
    if "yt_pre_videos" in df_buzz.columns:
        ax = axes[0]
        x = range(len(df_buzz))
        w = 0.35
        ax.bar([i - w/2 for i in x], df_buzz.get("yt_pre_videos", 0),
               width=w, label="방송 전", color="#3498db", alpha=0.8)
        ax.bar([i + w/2 for i in x], df_buzz.get("yt_post_videos", 0),
               width=w, label="방송 후", color="#e74c3c", alpha=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(df_buzz["방송"], rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("YouTube 영상 수")
        ax.set_title("YouTube 영상 수 (방송 전 vs 후)")
        ax.legend()
        ax.grid(True, alpha=0.3)

    # YouTube 조회수
    if "yt_pre_views" in df_buzz.columns:
        ax = axes[1]
        ax.bar([i - w/2 for i in x], df_buzz.get("yt_pre_views", 0),
               width=w, label="방송 전", color="#3498db", alpha=0.8)
        ax.bar([i + w/2 for i in x], df_buzz.get("yt_post_views", 0),
               width=w, label="방송 후", color="#e74c3c", alpha=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(df_buzz["방송"], rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("총 조회수")
        ax.set_title("YouTube 총 조회수 (방송 전 vs 후)")
        ax.legend()
        ax.grid(True, alpha=0.3)

    fig.suptitle("온라인 버즈 분석", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()
    return True


def plot_confounder_heatmap(df_conf_summary, save_path):
    """방송별 교란요소 히트맵"""
    if len(df_conf_summary) == 0:
        return False

    cols = []
    for c in ["temp_diff", "nice_diff", "pre_holidays", "post_holidays",
              "pre_events", "post_events", "pre_season_score_avg", "post_season_score_avg"]:
        if c in df_conf_summary.columns:
            cols.append(c)

    if not cols:
        return False

    fig, ax = plt.subplots(figsize=(10, 5))
    data = df_conf_summary[cols].fillna(0).values
    labels_y = df_conf_summary["방송"].values
    labels_x = cols

    im = ax.imshow(data, cmap="RdYlGn_r", aspect="auto")
    ax.set_xticks(range(len(labels_x)))
    ax.set_xticklabels(labels_x, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(range(len(labels_y)))
    ax.set_yticklabels(labels_y, fontsize=9)

    for i in range(len(labels_y)):
        for j in range(len(labels_x)):
            ax.text(j, i, f"{data[i, j]:.1f}", ha="center", va="center", fontsize=7)

    plt.colorbar(im, ax=ax, shrink=0.8)
    ax.set_title("방송별 교란요소 비교", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()
    return True


def plot_comprehensive_dashboard(save_path):
    """종합 대시보드 (4패널)"""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))

    # 패널1: 방송 타임라인
    ax = axes[0, 0]
    for i, bc in enumerate(BROADCASTS):
        air = pd.to_datetime(bc["air_date"])
        rating = bc.get("rating") or 0
        ax.barh(i, rating, color=plt.cm.Set2(i / len(BROADCASTS)), alpha=0.8)
        ax.text(rating + 0.1, i, f"{bc['name']} ({bc['air_date']})", va="center", fontsize=8)
    ax.set_yticks([])
    ax.set_xlabel("시청률 (%)")
    ax.set_title("방송 프로그램별 시청률")
    ax.grid(True, alpha=0.3, axis="x")

    # 패널2: 교란요소 요약
    ax = axes[0, 1]
    if len(df_confounder_summary) > 0 and "temp_diff" in df_confounder_summary.columns:
        x = range(len(df_confounder_summary))
        ax.bar(x, df_confounder_summary["temp_diff"].fillna(0), color="#e74c3c", alpha=0.7)
        ax.set_xticks(x)
        ax.set_xticklabels(df_confounder_summary["방송"], rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("기온 변화 (C)")
        ax.set_title("방송 전후 기온 변화")
        ax.axhline(0, color="black", linewidth=0.5)
        ax.grid(True, alpha=0.3)
    else:
        ax.text(0.5, 0.5, "교란요소 데이터 없음", ha="center", va="center", transform=ax.transAxes)

    # 패널3: T맵 DID 결과
    ax = axes[1, 0]
    if len(df_tmap_summary) > 0:
        cols_treat = [c for c in df_tmap_summary.columns if "treat" in c.lower() or "노출" in c]
        cols_ctrl = [c for c in df_tmap_summary.columns if "control" in c.lower() or "대조" in c]
        ax.text(0.5, 0.5, f"T맵 DID 결과\n(데이터 {len(df_tmap_summary)}행)",
                ha="center", va="center", transform=ax.transAxes, fontsize=12)
    elif len(df_did) > 0:
        ax.text(0.5, 0.5, f"DID 결과\n(데이터 {len(df_did)}행)",
                ha="center", va="center", transform=ax.transAxes, fontsize=12)
    else:
        ax.text(0.5, 0.5, "T맵/DID 데이터 없음\n(step2, step3 실행 필요)",
                ha="center", va="center", transform=ax.transAxes, fontsize=10)

    # 패널4: 온라인 버즈
    ax = axes[1, 1]
    if len(df_buzz_summary) > 0 and "yt_post_videos" in df_buzz_summary.columns:
        x = range(len(df_buzz_summary))
        ax.bar(x, df_buzz_summary["yt_post_videos"].fillna(0), color="#3498db", alpha=0.8)
        ax.set_xticks(x)
        ax.set_xticklabels(df_buzz_summary["방송"], rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("YouTube 영상 수")
        ax.set_title("방송 후 YouTube 반응")
        ax.grid(True, alpha=0.3)
    else:
        ax.text(0.5, 0.5, "온라인 버즈 데이터 없음\n(step6 실행 필요)",
                ha="center", va="center", transform=ax.transAxes, fontsize=10)

    fig.suptitle("아산시 방송 홍보 효과 종합 대시보드", fontsize=16, fontweight="bold")
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()
    return True


# 차트 생성
chart_weather = REPORT_DIR / "chart_weather_timeline.png"
chart_buzz = REPORT_DIR / "chart_buzz_comparison.png"
chart_heatmap = REPORT_DIR / "chart_confounder_heatmap.png"
chart_dashboard = REPORT_DIR / "chart_dashboard.png"

has_weather = plot_weather_timeline(df_confounders, BROADCASTS, chart_weather)
has_buzz = plot_buzz_comparison(df_buzz_summary, chart_buzz)
has_heatmap = plot_confounder_heatmap(df_confounder_summary, chart_heatmap)
has_dashboard = plot_comprehensive_dashboard(chart_dashboard)

print(f"  날씨 타임라인: {'OK' if has_weather else 'SKIP'}")
print(f"  버즈 비교: {'OK' if has_buzz else 'SKIP'}")
print(f"  교란요소 히트맵: {'OK' if has_heatmap else 'SKIP'}")
print(f"  종합 대시보드: {'OK' if has_dashboard else 'SKIP'}")

# ============================================================
# 3. PDF 리포트 생성 (reportlab)
# ============================================================

print("\n--- PDF 리포트 생성 ---")

try:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm, cm
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        Image, PageBreak, KeepTogether
    )
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    # 한글 폰트 등록
    if platform.system() == "Darwin":
        font_path = "/System/Library/Fonts/Supplemental/AppleGothic.ttf"
        if not Path(font_path).exists():
            font_path = "/System/Library/Fonts/AppleSDGothicNeo.ttc"
    elif platform.system() == "Windows":
        font_path = "C:/Windows/Fonts/malgun.ttf"
    else:
        font_path = None

    if font_path and Path(font_path).exists():
        pdfmetrics.registerFont(TTFont("Korean", font_path))
        FONT = "Korean"
    else:
        FONT = "Helvetica"

    # 스타일
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name="KorTitle", fontName=FONT, fontSize=20, leading=28,
        alignment=1, spaceAfter=20, textColor=colors.HexColor("#2c3e50")
    ))
    styles.add(ParagraphStyle(
        name="KorH1", fontName=FONT, fontSize=14, leading=20,
        spaceBefore=16, spaceAfter=8, textColor=colors.HexColor("#2c3e50"),
        borderWidth=0, borderPadding=0, borderColor=None,
    ))
    styles.add(ParagraphStyle(
        name="KorH2", fontName=FONT, fontSize=11, leading=16,
        spaceBefore=10, spaceAfter=6, textColor=colors.HexColor("#34495e"),
    ))
    styles.add(ParagraphStyle(
        name="KorBody", fontName=FONT, fontSize=9, leading=14,
        spaceAfter=6, textColor=colors.HexColor("#2c3e50"),
    ))
    styles.add(ParagraphStyle(
        name="KorSmall", fontName=FONT, fontSize=7, leading=10,
        textColor=colors.HexColor("#7f8c8d"),
    ))

    def make_table(headers, rows, col_widths=None):
        """깔끔한 테이블 생성"""
        data = [headers] + rows
        t = Table(data, colWidths=col_widths, repeatRows=1)
        t.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (-1, -1), FONT),
            ("FONTSIZE", (0, 0), (-1, 0), 8),
            ("FONTSIZE", (0, 1), (-1, -1), 7),
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2c3e50")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#bdc3c7")),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f9fa")]),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        return t

    # PDF 빌드
    pdf_path = OUTPUT_DIR / "아산시_방송홍보효과_종합리포트.pdf"
    doc = SimpleDocTemplate(
        str(pdf_path), pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm,
    )

    story = []

    # 표지
    story.append(Spacer(1, 80))
    story.append(Paragraph("아산시 방송 홍보 효과 종합 분석", styles["KorTitle"]))
    story.append(Spacer(1, 10))
    story.append(Paragraph(
        f"분석 기간: 2025-04 ~ 2026-05 | 생성일: {datetime.now().strftime('%Y-%m-%d')}",
        styles["KorBody"]
    ))
    story.append(Spacer(1, 30))

    # 분석 개요
    overview_text = """
    본 보고서는 아산시가 집행한 7건의 방송 홍보 프로그램에 대해,
    T맵 관광지 방문, SKT 유동인구, 아산페이 매출, 네이버 검색 트렌드,
    YouTube/블로그 반응 등 다각도 데이터를 활용하여 방송 효과를 분석한 종합 리포트입니다.
    날씨, 계절성, 공휴일, 지역 이벤트 등 교란요소를 통제하여
    순수 방송 효과를 분리하였습니다.
    """
    story.append(Paragraph(overview_text.strip(), styles["KorBody"]))
    story.append(PageBreak())

    # 1. 방송 프로그램 개요
    story.append(Paragraph("1. 분석 대상 방송 프로그램", styles["KorH1"]))
    bc_headers = ["방송명", "방송사", "방영일", "시청률(%)", "예산(천원)", "타겟"]
    bc_rows = []
    for bc in BROADCASTS:
        bc_rows.append([
            bc["name"][:12],
            bc["broadcaster"],
            bc["air_date"],
            str(bc.get("rating") or "-"),
            f"{bc.get('budget_1000won', 0):,}",
            bc.get("target_demo", "-"),
        ])
    story.append(make_table(bc_headers, bc_rows, col_widths=[80, 50, 65, 45, 55, 40]))
    story.append(Spacer(1, 15))

    # 2. 교란요소 분석
    story.append(Paragraph("2. 교란요소 분석", styles["KorH1"]))
    story.append(Paragraph(
        "방송 효과를 정확히 측정하기 위해, 각 방송의 전후 기간에 존재하는 "
        "날씨(기온/강수/적설), 공휴일, 계절성, 지역 이벤트 등 교란요소를 수집하고 통제하였습니다.",
        styles["KorBody"]
    ))

    if has_weather:
        story.append(Spacer(1, 10))
        story.append(Image(str(chart_weather), width=450, height=280))

    if len(df_confounder_summary) > 0:
        story.append(Spacer(1, 10))
        conf_headers = ["방송", "기온차(C)", "쾌적일차", "공휴일(전)", "공휴일(후)", "주요교란"]
        conf_rows = []
        for _, r in df_confounder_summary.iterrows():
            conf_rows.append([
                str(r.get("방송", ""))[:12],
                f"{r.get('temp_diff', 0):.1f}" if pd.notna(r.get("temp_diff")) else "-",
                f"{r.get('nice_diff', 0):.0f}" if pd.notna(r.get("nice_diff")) else "-",
                str(int(r.get("pre_holidays", 0))),
                str(int(r.get("post_holidays", 0))),
                str(r.get("known_confounders", "-"))[:20],
            ])
        story.append(make_table(conf_headers, conf_rows, col_widths=[75, 50, 50, 50, 50, 100]))

    if has_heatmap:
        story.append(Spacer(1, 10))
        story.append(Image(str(chart_heatmap), width=400, height=200))

    story.append(PageBreak())

    # 3. 온라인 버즈 분석
    story.append(Paragraph("3. 온라인 버즈 분석 (네이버/YouTube)", styles["KorH1"]))
    story.append(Paragraph(
        "네이버 블로그/뉴스 게시물 수, DataLab 검색 트렌드, YouTube 영상 수/조회수/댓글을 "
        "수집하여 방송 전후 온라인 반응 변화를 분석하였습니다.",
        styles["KorBody"]
    ))

    if len(df_buzz_summary) > 0:
        buzz_headers = ["방송", "YT전(영상)", "YT후(영상)", "YT전(조회)", "YT후(조회)"]
        buzz_rows = []
        for _, r in df_buzz_summary.iterrows():
            buzz_rows.append([
                str(r.get("방송", ""))[:12],
                str(int(r.get("yt_pre_videos", 0))),
                str(int(r.get("yt_post_videos", 0))),
                f"{int(r.get('yt_pre_views', 0)):,}",
                f"{int(r.get('yt_post_views', 0)):,}",
            ])
        story.append(make_table(buzz_headers, buzz_rows, col_widths=[80, 60, 60, 80, 80]))

    if has_buzz:
        story.append(Spacer(1, 10))
        story.append(Image(str(chart_buzz), width=450, height=200))

    story.append(PageBreak())

    # 4. 기존 분석 결과 요약 (T맵/카드/DID)
    story.append(Paragraph("4. 방문/소비 데이터 분석 결과", styles["KorH1"]))
    story.append(Paragraph(
        "T맵 관광지 목적지 검색, SKT 유동인구, 아산페이 카드매출 데이터를 활용한 "
        "DID(이중차분법) 분석 결과를 종합합니다.",
        styles["KorBody"]
    ))

    if len(df_tmap_summary) > 0:
        story.append(Paragraph("4.1 T맵 관광지 방문 효과", styles["KorH2"]))
        # T맵 데이터 표
        tmap_cols = df_tmap_summary.columns.tolist()
        tmap_headers = [c[:10] for c in tmap_cols]
        tmap_rows = []
        for _, r in df_tmap_summary.head(10).iterrows():
            tmap_rows.append([str(r[c])[:12] for c in tmap_cols])
        story.append(make_table(tmap_headers, tmap_rows))

    if len(df_did) > 0:
        story.append(Spacer(1, 10))
        story.append(Paragraph("4.2 DID(이중차분법) 결과", styles["KorH2"]))
        did_cols = df_did.columns.tolist()[:6]
        did_headers = [c[:12] for c in did_cols]
        did_rows = []
        for _, r in df_did.head(10).iterrows():
            did_rows.append([str(r[c])[:15] for c in did_cols])
        story.append(make_table(did_headers, did_rows))

    story.append(PageBreak())

    # 5. 종합 판정
    story.append(Paragraph("5. 종합 판정 및 결론", styles["KorH1"]))

    conclusions = []
    for bc in BROADCASTS:
        name = bc["name"]
        rating = bc.get("rating") or 0
        budget = bc.get("budget_1000won", 0)

        # 교란요소 판정
        conf_row = df_confounder_summary[df_confounder_summary["방송"] == name] if len(df_confounder_summary) > 0 else pd.DataFrame()
        conf_risk = "미분석"
        if len(conf_row) > 0:
            td = conf_row.iloc[0].get("temp_diff", 0)
            if pd.isna(td):
                td = 0
            if abs(td) > 10:
                conf_risk = "높음 (계절 전환)"
            elif abs(td) > 5:
                conf_risk = "중간"
            else:
                conf_risk = "낮음"

        # 온라인 버즈 판정
        buzz_row = df_buzz_summary[df_buzz_summary["방송"] == name] if len(df_buzz_summary) > 0 else pd.DataFrame()
        buzz_effect = "미분석"
        if len(buzz_row) > 0:
            pre_v = buzz_row.iloc[0].get("yt_pre_videos", 0)
            post_v = buzz_row.iloc[0].get("yt_post_videos", 0)
            if post_v > pre_v * 2:
                buzz_effect = "강한 반응"
            elif post_v > pre_v:
                buzz_effect = "약한 반응"
            else:
                buzz_effect = "반응 없음"

        conclusions.append([
            name[:12], f"{rating}%", f"{budget:,}천원",
            conf_risk, buzz_effect
        ])

    story.append(make_table(
        ["방송명", "시청률", "예산", "교란위험", "온라인반응"],
        conclusions,
        col_widths=[80, 50, 70, 70, 70]
    ))

    story.append(Spacer(1, 15))
    story.append(Paragraph(
        "주요 시사점:",
        styles["KorH2"]
    ))

    insights = [
        "1. 뛰어야산다2는 겨울 비수기 방영으로 교란요소가 가장 적어 클린 비교가 가능한 최적 케이스",
        "2. 11월 밀집 방송(전현무/굿모닝/6시내고향/같이삽시다)은 가을 단풍 시즌과 겹쳐 순수 효과 분리 어려움",
        "3. 황제파워는 이순신축제와 동시 진행으로 방송 단독 효과 측정 제한적",
        "4. T맵 2026 데이터 기반 분석에서 노출 관광지 방문은 증가 추세이나 통계적 유의성 확보 필요",
        "5. 추가 데이터(T맵 전년 동기, 아산페이 일별, 관광지 입장객) 확보 시 결론 보강 가능",
    ]
    for ins in insights:
        story.append(Paragraph(ins, styles["KorBody"]))

    # 종합 대시보드
    if has_dashboard:
        story.append(PageBreak())
        story.append(Paragraph("6. 종합 대시보드", styles["KorH1"]))
        story.append(Image(str(chart_dashboard), width=470, height=350))

    # 부록
    story.append(PageBreak())
    story.append(Paragraph("부록: 분석 방법론", styles["KorH1"]))
    methods = [
        "DID (이중차분법): 방송 노출 지역(처치군)과 미노출 지역(대조군)의 전후 변화 차이를 비교하여 순수 방송 효과 추정",
        "교란요소 통제: 날씨(기온/강수/적설), 요일(주말/공휴일), 계절성(관광시즌 점수), 지역이벤트를 회귀모델에 포함",
        "온라인 버즈: 네이버 블로그/뉴스 게시물 수, DataLab 검색 트렌드, YouTube 영상 수/조회수를 방송 전후 비교",
        "데이터 소스: T맵 관광지 목적지 검색(2019~2026), SKT 유동인구(2026-01~02), 아산페이 카드매출(2026-01~02), "
        "Open-Meteo 날씨 데이터, 네이버 검색 API, YouTube Data API v3",
    ]
    for m in methods:
        story.append(Paragraph(f"- {m}", styles["KorBody"]))

    story.append(Spacer(1, 30))
    story.append(Paragraph(
        f"Generated by 몽이 AI | {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        styles["KorSmall"]
    ))

    # 빌드
    doc.build(story)
    print(f"\n  PDF 생성 완료: {pdf_path}")

except ImportError:
    print("  [!] reportlab 미설치. pip install reportlab 후 재실행")
    print("  대신 마크다운 리포트 생성...")

    # Markdown 리포트 대체
    md_path = OUTPUT_DIR / "아산시_방송홍보효과_종합리포트.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# 아산시 방송 홍보 효과 종합 분석\n\n")
        f.write(f"분석 기간: 2025-04 ~ 2026-05 | 생성일: {datetime.now().strftime('%Y-%m-%d')}\n\n")
        f.write("## 1. 분석 대상\n\n")
        for bc in BROADCASTS:
            f.write(f"- **{bc['name']}** ({bc['broadcaster']}, {bc['air_date']}, 시청률 {bc.get('rating', '-')}%)\n")
        f.write("\n## 2. 교란요소 분석\n\n")
        if len(df_confounder_summary) > 0:
            f.write(df_confounder_summary.to_markdown(index=False))
        f.write("\n\n## 3. 온라인 버즈\n\n")
        if len(df_buzz_summary) > 0:
            f.write(df_buzz_summary.to_markdown(index=False))
        f.write("\n\n## 4. 결론\n\n")
        f.write("교란요소 통제 후 종합 판정은 PDF 버전 참고\n")
    print(f"  마크다운 리포트 생성: {md_path}")

print(f"\n{'=' * 60}")
print("종합 리포트 생성 완료!")
print(f"{'=' * 60}")
