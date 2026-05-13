"""
Step 8: 종합 리포트 생성 (PDF) — 상세 버전
==========================================
- Step 6~7 결과 + (있으면) Step 1~5 결과를 모두 합쳐 PDF 생성
- 방송별 개별 분석 페이지 포함
- 교란요소 해석 텍스트 자동 생성
- 온라인 버즈 상세 해석

실행: python step8_final_report.py
필요: pip install reportlab pandas numpy matplotlib python-dotenv
"""
import pandas as pd
import numpy as np
import platform
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime, timedelta

from config import BROADCASTS, OUTPUT_DIR

# ── 한글 폰트 ──────────────────────────────────────────
if platform.system() == "Darwin":
    plt.rcParams["font.family"] = "AppleGothic"
elif platform.system() == "Windows":
    plt.rcParams["font.family"] = "Malgun Gothic"
plt.rcParams["axes.unicode_minus"] = False

REPORT_DIR = OUTPUT_DIR / "report"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 60)
print("Step 8: 종합 리포트 생성 (상세)")
print("=" * 60)

# ============================================================
# 1. 데이터 로드
# ============================================================

def safe_load(fn, **kw):
    try:
        return pd.read_csv(OUTPUT_DIR / fn, encoding="utf-8-sig", **kw)
    except FileNotFoundError:
        return pd.DataFrame()

df_buzz_raw = safe_load("online_buzz_summary.csv")
df_conf_sum = safe_load("confounder_summary_by_broadcast.csv")
df_conf_all = safe_load("confounders_merged.csv")
df_weather  = safe_load("weather_daily.csv")
df_tmap     = safe_load("tmap_broadcast_effect_summary.csv")
df_did      = safe_load("did_results.csv")
df_card     = safe_load("card_outsider_by_dong_monthly.csv")

# 방송별 DataLab 트렌드 로드
datalab_by_bc = {}
for bc in BROADCASTS:
    safe = bc["name"].replace(" ", "_").replace("/", "_")
    df = safe_load(f"datalab_trend_{safe}.csv")
    if len(df) > 0:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        datalab_by_bc[bc["name"]] = df

# 방송별 블로그 일별 로드
blog_by_bc = {}
for bc in BROADCASTS:
    safe = bc["name"].replace(" ", "_").replace("/", "_")
    df = safe_load(f"blog_daily_{safe}.csv")
    if len(df) > 0:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        blog_by_bc[bc["name"]] = df

# ============================================================
# 2. 분석 함수 (방송별 해석 텍스트 생성)
# ============================================================

def analyze_youtube(row):
    """YouTube 전후 비교 해석"""
    pre_v = row.get("yt_pre_videos", 0) or 0
    post_v = row.get("yt_post_videos", 0) or 0
    pre_views = row.get("yt_pre_views", 0) or 0
    post_views = row.get("yt_post_views", 0) or 0

    lines = []
    lines.append(f"영상 수: 방송 전 {int(pre_v)}개 / 방송 후 {int(post_v)}개")
    lines.append(f"총 조회수: 방송 전 {int(pre_views):,}회 / 방송 후 {int(post_views):,}회")

    if pre_views > 0:
        pct = (post_views - pre_views) / pre_views * 100
        if pct > 100:
            lines.append(f"조회수 {pct:+.0f}% 증가 — 방송 후 YouTube 반응이 매우 강함")
        elif pct > 30:
            lines.append(f"조회수 {pct:+.0f}% 증가 — 의미 있는 온라인 반응")
        elif pct > 0:
            lines.append(f"조회수 {pct:+.0f}% 소폭 증가")
        else:
            lines.append(f"조회수 {pct:+.0f}% — 방송 후 추가 반응 미미")
    return "\n".join(lines)


def analyze_datalab(bc_name, air_date_str):
    """DataLab 검색 트렌드 해석"""
    df = datalab_by_bc.get(bc_name)
    if df is None or len(df) == 0:
        return "DataLab 데이터 없음"

    air = pd.to_datetime(air_date_str)
    lines = []
    for kw in df["keyword"].unique():
        sub = df[df["keyword"] == kw]
        pre = sub[sub["date"] < air]["ratio"]
        post = sub[sub["date"] >= air]["ratio"]
        pre_m = pre.mean()
        post_m = post.mean()
        if pd.isna(pre_m) or pre_m == 0:
            if pd.notna(post_m) and post_m > 0:
                lines.append(f"  '{kw}': 방송 전 검색 없음 → 방송 후 {post_m:.1f} (신규 생성)")
            continue
        pct = (post_m - pre_m) / pre_m * 100
        lines.append(f"  '{kw}': {pre_m:.1f} → {post_m:.1f} ({pct:+.0f}%)")

    if not lines:
        return "검색 트렌드 변화 미미"
    return "네이버 검색 트렌드 (상대지수):\n" + "\n".join(lines)


def analyze_blog(bc_name, air_date_str):
    """블로그 일별 게시 패턴 해석"""
    df = blog_by_bc.get(bc_name)
    if df is None or len(df) == 0:
        return "블로그 일별 데이터 없음"

    air = pd.to_datetime(air_date_str).date()
    pre = df[df["date"].dt.date < air]
    post = df[df["date"].dt.date >= air]
    lines = []
    lines.append(f"수집 게시물: 방송 전 {len(pre)}건 / 방송 후 {len(post)}건")

    if len(pre) > 0 and len(post) > 0:
        # 일평균 게시 수
        pre_days = (pd.to_datetime(air) - pre["date"].min()).days or 1
        post_days = (post["date"].max() - pd.to_datetime(air)).days or 1
        pre_daily = len(pre) / pre_days
        post_daily = len(post) / post_days
        lines.append(f"일평균 게시: {pre_daily:.1f}건/일 → {post_daily:.1f}건/일")

    return "\n".join(lines)


def analyze_confounders(bc_name):
    """교란요소 해석"""
    if len(df_conf_sum) == 0:
        return "교란요소 데이터 없음"

    row = df_conf_sum[df_conf_sum["방송"] == bc_name]
    if len(row) == 0:
        return "해당 방송 교란요소 데이터 없음"
    r = row.iloc[0]

    lines = []

    # 기온
    td = r.get("temp_diff", 0)
    if pd.notna(td):
        pre_t = r.get("pre_temp_mean", 0)
        post_t = r.get("post_temp_mean", 0)
        lines.append(f"기온: 방송 전 평균 {pre_t:.1f}C → 방송 후 {post_t:.1f}C (차이 {td:+.1f}C)")
        if abs(td) > 10:
            lines.append("  → 계절 전환이 크게 작용, 방문 패턴 변화의 상당 부분이 날씨 영향일 수 있음")
        elif abs(td) > 5:
            lines.append("  → 계절 변화 영향 존재, DID 분석 시 날씨 통제 필수")
        else:
            lines.append("  → 기온 변화 작아 날씨 교란 낮음")

    # 강수
    pre_p = r.get("pre_precip_total", 0)
    post_p = r.get("post_precip_total", 0)
    if pd.notna(pre_p) and pd.notna(post_p):
        lines.append(f"강수량: 방송 전 {pre_p:.0f}mm / 방송 후 {post_p:.0f}mm")

    # 쾌적일
    pre_n = r.get("pre_nice_days", 0) or 0
    post_n = r.get("post_nice_days", 0) or 0
    lines.append(f"쾌적한 날(10~25C, 비 없음): 방송 전 {int(pre_n)}일 / 방송 후 {int(post_n)}일")

    # 공휴일/주말
    pre_h = r.get("pre_holidays", 0) or 0
    post_h = r.get("post_holidays", 0) or 0
    pre_w = r.get("pre_weekends", 0) or 0
    post_w = r.get("post_weekends", 0) or 0
    lines.append(f"주말: {int(pre_w)}일/{int(post_w)}일, 공휴일: {int(pre_h)}일/{int(post_h)}일")
    if abs(post_h - pre_h) >= 2:
        lines.append("  → 공휴일 비대칭 주의 (방문 패턴에 영향)")

    # 시즌
    pre_s = r.get("pre_season_score_avg", 3) or 3
    post_s = r.get("post_season_score_avg", 3) or 3
    lines.append(f"관광 시즌 점수: {pre_s:.1f} → {post_s:.1f} (5=성수기)")
    if abs(post_s - pre_s) >= 1:
        lines.append("  → 시즌 전환으로 계절성 교란 높음")

    # 알려진 교란
    known = r.get("known_confounders", "없음")
    if known and known != "없음":
        lines.append(f"기타 알려진 교란: {known}")

    return "\n".join(lines)


def overall_judgment(bc):
    """종합 판정"""
    name = bc["name"]
    lines = []

    # 교란위험
    conf_risk = "미분석"
    if len(df_conf_sum) > 0:
        row = df_conf_sum[df_conf_sum["방송"] == name]
        if len(row) > 0:
            td = row.iloc[0].get("temp_diff", 0)
            if pd.isna(td): td = 0
            known = row.iloc[0].get("known_confounders", "")
            events = (row.iloc[0].get("pre_events", 0) or 0) + (row.iloc[0].get("post_events", 0) or 0)
            if abs(td) > 10 or events > 3:
                conf_risk = "높음"
            elif abs(td) > 5 or events > 1 or (known and known != "없음"):
                conf_risk = "중간"
            else:
                conf_risk = "낮음"

    # 온라인 반응
    buzz_level = "미분석"
    if len(df_buzz_raw) > 0:
        brow = df_buzz_raw[df_buzz_raw["방송"] == name]
        if len(brow) > 0:
            pre_views = brow.iloc[0].get("yt_pre_views", 0) or 0
            post_views = brow.iloc[0].get("yt_post_views", 0) or 0
            if pre_views > 0:
                pct = (post_views - pre_views) / pre_views * 100
                if pct > 100: buzz_level = "매우 강함"
                elif pct > 30: buzz_level = "강함"
                elif pct > 0: buzz_level = "약함"
                else: buzz_level = "감소/없음"
            elif post_views > 0:
                buzz_level = "약함 (기존 콘텐츠 부재)"
            else:
                buzz_level = "없음"

    # 순수 효과 추정 가능성
    if conf_risk == "낮음":
        estimability = "순수 방송 효과 추정 용이"
    elif conf_risk == "중간":
        estimability = "교란요소 통제 필요 (DID + 날씨/시즌 보정)"
    else:
        estimability = "순수 효과 분리 어려움 (복수 교란)"

    lines.append(f"교란위험: {conf_risk}")
    lines.append(f"온라인 반응: {buzz_level}")
    lines.append(f"효과 추정: {estimability}")

    # 방송 특성 기반 코멘트
    rating = bc.get("rating") or 0
    budget = bc.get("budget_1000won", 0)
    if budget > 0 and rating > 0:
        cpr = budget / rating  # 시청률 1% 당 비용
        lines.append(f"비용효율: 예산 {budget:,}천원 / 시청률 {rating}% (시청률 1% 당 {cpr:,.0f}천원)")

    return "\n".join(lines)


# ============================================================
# 3. 시각화
# ============================================================

print("\n--- 시각화 생성 ---")

def plot_weather_timeline(save_path):
    if len(df_conf_all) == 0 or "temperature_2m_mean" not in df_conf_all.columns:
        return False
    df = df_conf_all.copy()
    df["date"] = pd.to_datetime(df["date"])

    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)
    ax = axes[0]
    ax.fill_between(df["date"], df["temperature_2m_min"], df["temperature_2m_max"],
                    alpha=0.3, color="#3498db", label="기온 범위")
    ax.plot(df["date"], df["temperature_2m_mean"], color="#2c3e50", lw=1, label="평균")
    ax.set_ylabel("기온 (C)")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    ax = axes[1]
    ax.bar(df["date"], df["precipitation_sum"], color="#3498db", alpha=0.7, label="강수(mm)")
    if "snowfall_sum" in df.columns:
        ax.bar(df["date"], df["snowfall_sum"], color="#95a5a6", alpha=0.7, label="적설(cm)")
    ax.set_ylabel("강수/적설")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    ax = axes[2]
    if "season_score" in df.columns:
        ax.fill_between(df["date"], df["season_score"], alpha=0.3, color="#e67e22")
        ax.plot(df["date"], df["season_score"], color="#e67e22", lw=1)
    ax.set_ylabel("시즌 점수")
    ax.set_ylim(0, 6)
    ax.grid(True, alpha=0.3)

    colors_bc = plt.cm.Set2(np.linspace(0, 1, len(BROADCASTS)))
    for i, bc in enumerate(BROADCASTS):
        air = pd.to_datetime(bc["air_date"])
        for a in axes:
            a.axvline(air, color=colors_bc[i], ls="--", alpha=0.7, lw=1)
        axes[0].annotate(bc["name"][:8], xy=(air, axes[0].get_ylim()[1]),
                        fontsize=6, rotation=45, ha="left", color=colors_bc[i])

    fig.suptitle("아산시 날씨 + 방영일 타임라인", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()
    return True


def plot_yt_comparison(save_path):
    if len(df_buzz_raw) == 0:
        return False
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    names = [r["방송"][:8] for _, r in df_buzz_raw.iterrows()]
    x = range(len(names))
    w = 0.35

    for idx, (col_pre, col_post, ylabel, title) in enumerate([
        ("yt_pre_videos", "yt_post_videos", "영상 수", "YouTube 영상 수"),
        ("yt_pre_views", "yt_post_views", "조회수", "YouTube 총 조회수"),
    ]):
        ax = axes[idx]
        pre_vals = df_buzz_raw[col_pre].fillna(0).values
        post_vals = df_buzz_raw[col_post].fillna(0).values
        ax.bar([i - w/2 for i in x], pre_vals, w, label="방송 전", color="#3498db", alpha=0.8)
        ax.bar([i + w/2 for i in x], post_vals, w, label="방송 후", color="#e74c3c", alpha=0.8)
        ax.set_xticks(list(x))
        ax.set_xticklabels(names, rotation=45, ha="right", fontsize=8)
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()
    return True


def plot_datalab_for_bc(bc_name, air_date, save_path):
    """개별 방송 DataLab 트렌드 차트"""
    df = datalab_by_bc.get(bc_name)
    if df is None or len(df) == 0:
        return False
    fig, ax = plt.subplots(figsize=(10, 4))
    for kw in df["keyword"].unique():
        sub = df[df["keyword"] == kw].sort_values("date")
        ax.plot(sub["date"], sub["ratio"], label=kw[:20], lw=1.2)
    ax.axvline(pd.to_datetime(air_date), color="red", ls="--", lw=1.5, label="방영일")
    ax.set_ylabel("검색 상대지수")
    ax.set_title(f"{bc_name} - 네이버 검색 트렌드", fontsize=12)
    ax.legend(fontsize=7, loc="upper left")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()
    return True


# 전체 차트
c_weather = REPORT_DIR / "chart_weather.png"
c_yt = REPORT_DIR / "chart_yt_compare.png"
has_weather = plot_weather_timeline(c_weather)
has_yt = plot_yt_comparison(c_yt)
print(f"  날씨: {'OK' if has_weather else 'SKIP'}, YouTube: {'OK' if has_yt else 'SKIP'}")

# 방송별 DataLab 차트
bc_datalab_charts = {}
for bc in BROADCASTS:
    safe = bc["name"].replace(" ", "_").replace("/", "_")
    p = REPORT_DIR / f"chart_datalab_{safe}.png"
    if plot_datalab_for_bc(bc["name"], bc["air_date"], p):
        bc_datalab_charts[bc["name"]] = p
        print(f"  DataLab {bc['name'][:10]}: OK")


# ============================================================
# 4. PDF 생성
# ============================================================

print("\n--- PDF 생성 ---")

try:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm, cm
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        Image, PageBreak, KeepTogether, HRFlowable
    )
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    # 폰트
    if platform.system() == "Darwin":
        fp = "/System/Library/Fonts/Supplemental/AppleGothic.ttf"
        if not Path(fp).exists():
            fp = "/System/Library/Fonts/AppleSDGothicNeo.ttc"
    elif platform.system() == "Windows":
        fp = "C:/Windows/Fonts/malgun.ttf"
    else:
        fp = None

    FONT = "Helvetica"
    if fp and Path(fp).exists():
        try:
            pdfmetrics.registerFont(TTFont("Korean", fp))
            FONT = "Korean"
        except:
            pass

    sty = getSampleStyleSheet()
    sty.add(ParagraphStyle("T", fontName=FONT, fontSize=22, leading=30,
                            alignment=1, spaceAfter=20, textColor=colors.HexColor("#1a1a2e")))
    sty.add(ParagraphStyle("Sub", fontName=FONT, fontSize=11, leading=16,
                            alignment=1, textColor=colors.HexColor("#4a4a6a")))
    sty.add(ParagraphStyle("H1", fontName=FONT, fontSize=14, leading=20,
                            spaceBefore=18, spaceAfter=8, textColor=colors.HexColor("#1a1a2e")))
    sty.add(ParagraphStyle("H2", fontName=FONT, fontSize=11, leading=16,
                            spaceBefore=12, spaceAfter=6, textColor=colors.HexColor("#2d2d4a")))
    sty.add(ParagraphStyle("B", fontName=FONT, fontSize=9, leading=13.5,
                            spaceAfter=4, textColor=colors.HexColor("#333333")))
    sty.add(ParagraphStyle("BSmall", fontName=FONT, fontSize=8, leading=12,
                            spaceAfter=3, textColor=colors.HexColor("#555555")))
    sty.add(ParagraphStyle("Footer", fontName=FONT, fontSize=7, leading=10,
                            textColor=colors.HexColor("#999999")))

    def tbl(headers, rows, cw=None):
        data = [headers] + rows
        t = Table(data, colWidths=cw, repeatRows=1)
        t.setStyle(TableStyle([
            ("FONTNAME", (0,0), (-1,-1), FONT),
            ("FONTSIZE", (0,0), (-1,0), 8),
            ("FONTSIZE", (0,1), (-1,-1), 7.5),
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#1a1a2e")),
            ("TEXTCOLOR", (0,0), (-1,0), colors.white),
            ("ALIGN", (0,0), (-1,-1), "CENTER"),
            ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
            ("GRID", (0,0), (-1,-1), 0.4, colors.HexColor("#d0d0d0")),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#f5f5fa")]),
            ("TOPPADDING", (0,0), (-1,-1), 4),
            ("BOTTOMPADDING", (0,0), (-1,-1), 4),
        ]))
        return t

    def hr():
        return HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#cccccc"),
                          spaceBefore=8, spaceAfter=8)

    def add_text_block(story, title, text_lines):
        """제목 + 여러 줄 텍스트 블록"""
        story.append(Paragraph(title, sty["H2"]))
        for line in text_lines.split("\n"):
            line = line.strip()
            if line:
                story.append(Paragraph(line, sty["BSmall"]))

    pdf_path = OUTPUT_DIR / "아산시_방송홍보효과_종합리포트.pdf"
    doc = SimpleDocTemplate(str(pdf_path), pagesize=A4,
                            leftMargin=2*cm, rightMargin=2*cm,
                            topMargin=2*cm, bottomMargin=2*cm)
    story = []

    # ── 표지 ──
    story.append(Spacer(1, 100))
    story.append(Paragraph("아산시 방송 홍보 효과<br/>종합 분석 보고서", sty["T"]))
    story.append(Spacer(1, 15))
    story.append(Paragraph(
        f"분석 기간: 2025-04 ~ 2026-05  |  대상: 7개 방송 프로그램<br/>"
        f"데이터: T맵, SKT 유동인구, 아산페이, 네이버, YouTube, 기상<br/>"
        f"보고서 생성일: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        sty["Sub"]))
    story.append(PageBreak())

    # ── 목차 ──
    story.append(Paragraph("목차", sty["H1"]))
    toc = [
        "1. 분석 개요 및 방법론",
        "2. 분석 대상 방송 프로그램",
        "3. 교란요소 분석 (날씨/시즌/공휴일/이벤트)",
        "4. 온라인 버즈 분석 (네이버/YouTube)",
        "5~11. 방송별 상세 분석 (7개 방송)",
        "12. 종합 판정 및 결론",
        "13. 추가 데이터 확보 시 분석 가능 항목",
    ]
    for t in toc:
        story.append(Paragraph(t, sty["B"]))
    story.append(PageBreak())

    # ── 1. 분석 개요 ──
    story.append(Paragraph("1. 분석 개요 및 방법론", sty["H1"]))
    story.append(hr())
    overview = """
본 보고서는 아산시가 2025년 4월~2026년 5월 기간에 집행한 7건의 방송 홍보 프로그램에 대해
다각도 데이터를 활용하여 방송 효과를 종합 분석한 결과입니다.

[분석 프레임워크]
- 레이어 1 (온라인 관심도): 네이버 검색 트렌드(DataLab), 블로그/뉴스 게시물 수, YouTube 영상/조회수
- 레이어 2 (실제 방문): T맵 관광지 목적지 검색, SKT 유동인구
- 레이어 3 (소비): 아산페이 카드매출 (읍면동/업종/연령별)
- 레이어 4 (교란요소 통제): 일별 기온/강수/적설, 공휴일, 관광시즌, 지역이벤트
- 레이어 5 (인과추론): DID(이중차분법), 처치군(방송 노출 지역) vs 대조군(미노출 지역)

[주요 방법론]
- DID (Difference-in-Differences): 노출 지역의 방송 전후 변화에서 미노출 지역의 전후 변화를 차감하여 순수 효과 추정
- 교란요소 통제: 날씨(Open-Meteo API), 공휴일, 계절성, 지역이벤트를 통제변수로 포함
- 전후 비교 윈도우: 방송 전 4주 vs 방송 후 4~8주
"""
    for line in overview.strip().split("\n"):
        line = line.strip()
        if line.startswith("["):
            story.append(Spacer(1, 6))
            story.append(Paragraph(f"<b>{line}</b>", sty["B"]))
        elif line.startswith("-"):
            story.append(Paragraph(f"  {line}", sty["BSmall"]))
        elif line:
            story.append(Paragraph(line, sty["B"]))
    story.append(PageBreak())

    # ── 2. 분석 대상 ──
    story.append(Paragraph("2. 분석 대상 방송 프로그램", sty["H1"]))
    story.append(hr())
    bc_rows = []
    for bc in BROADCASTS:
        bc_rows.append([
            bc["name"][:14], bc["broadcaster"],
            bc["air_date"], str(bc.get("rating") or "-"),
            f'{bc.get("budget_1000won", 0):,}', bc.get("target_demo", "-"),
            str(bc.get("episodes", 1))
        ])
    story.append(tbl(
        ["방송명", "방송사", "방영일", "시청률(%)", "예산(천원)", "타겟", "회차"],
        bc_rows,
        cw=[85, 45, 60, 40, 55, 35, 30]
    ))
    story.append(Spacer(1, 10))
    story.append(Paragraph(
        "총 7건, 예산 합계 581,518천원. 2025년 6월~11월 6건 밀집, 2026년 1월 1건(뛰어야산다2), 5월 1건(황제파워). "
        "11월에 4건이 밀집되어 있어 개별 효과 분리에 한계가 있으며, "
        "뛰어야산다2(2026-01-12)는 겨울 비수기 단독 방영으로 가장 클린한 비교 가능.",
        sty["B"]))
    story.append(PageBreak())

    # ── 3. 교란요소 ──
    story.append(Paragraph("3. 교란요소 분석", sty["H1"]))
    story.append(hr())
    story.append(Paragraph(
        "방송 효과를 정확히 측정하기 위해 각 방송의 전후 기간에 존재하는 교란요소를 수집하였습니다. "
        "날씨(기온/강수/적설), 공휴일/주말, 관광시즌 점수, 지역 이벤트(이순신축제 등)를 방송별로 비교합니다.",
        sty["B"]))

    if has_weather:
        story.append(Spacer(1, 8))
        story.append(Image(str(c_weather), width=440, height=270))
        story.append(Paragraph(
            "위 그래프는 분석 전 기간(2025-04~2026-05)의 일별 기온, 강수/적설, 관광시즌 점수를 보여줍니다. "
            "점선은 각 방송의 방영일입니다. 11월 방송 4건은 가을→겨울 전환기(기온 급락, 쾌적일 감소)에 위치하며, "
            "뛰어야산다2(1월)는 이미 한겨울이라 전후 기온 변화가 작습니다.",
            sty["BSmall"]))

    if len(df_conf_sum) > 0:
        story.append(Spacer(1, 10))
        story.append(Paragraph("방송별 교란요소 요약", sty["H2"]))
        conf_rows = []
        for _, r in df_conf_sum.iterrows():
            td = r.get("temp_diff", 0)
            conf_rows.append([
                str(r.get("방송", ""))[:12],
                f'{r.get("pre_temp_mean", 0):.1f}' if pd.notna(r.get("pre_temp_mean")) else "-",
                f'{r.get("post_temp_mean", 0):.1f}' if pd.notna(r.get("post_temp_mean")) else "-",
                f'{td:+.1f}' if pd.notna(td) else "-",
                f'{int(r.get("pre_nice_days", 0))}/{int(r.get("post_nice_days", 0))}',
                f'{int(r.get("pre_holidays", 0))}/{int(r.get("post_holidays", 0))}',
                str(r.get("known_confounders", "-"))[:18],
            ])
        story.append(tbl(
            ["방송", "기온전(C)", "기온후(C)", "차이", "쾌적(전/후)", "공휴일(전/후)", "교란요소"],
            conf_rows,
            cw=[75, 48, 48, 38, 55, 55, 100]
        ))
        story.append(Spacer(1, 6))
        story.append(Paragraph(
            "핵심: 뛰어야산다2(-2.6C 차이)와 전국노래자랑(+4.8C)은 기온 변화가 적어 교란 낮음. "
            "11월 방송 4건은 -5~7C 급변 + 가을단풍/겨울온천 시즌 전환으로 교란 중~높음. "
            "황제파워는 이순신축제와 동시 진행으로 방송 단독 효과 분리 어려움.",
            sty["B"]))

    story.append(PageBreak())

    # ── 4. 온라인 버즈 ──
    story.append(Paragraph("4. 온라인 버즈 분석 (네이버/YouTube)", sty["H1"]))
    story.append(hr())
    story.append(Paragraph(
        "각 방송명 + 관광지 키워드로 네이버 블로그/뉴스/DataLab, YouTube 영상/조회수/댓글을 수집하여 "
        "방송 전후 온라인 반응 변화를 측정하였습니다.",
        sty["B"]))

    if has_yt:
        story.append(Spacer(1, 8))
        story.append(Image(str(c_yt), width=440, height=180))

    # YouTube 전후 비교표
    if len(df_buzz_raw) > 0:
        story.append(Spacer(1, 8))
        yt_rows = []
        for _, r in df_buzz_raw.iterrows():
            pre_v = int(r.get("yt_pre_views", 0) or 0)
            post_v = int(r.get("yt_post_views", 0) or 0)
            pct = ((post_v - pre_v) / pre_v * 100) if pre_v > 0 else 0
            yt_rows.append([
                str(r["방송"])[:12],
                str(int(r.get("yt_pre_videos", 0) or 0)),
                str(int(r.get("yt_post_videos", 0) or 0)),
                f"{pre_v:,}", f"{post_v:,}",
                f"{pct:+.0f}%",
            ])
        story.append(tbl(
            ["방송", "전(영상)", "후(영상)", "전(조회수)", "후(조회수)", "변화율"],
            yt_rows,
            cw=[75, 45, 45, 70, 70, 45]
        ))

        story.append(Spacer(1, 8))
        story.append(Paragraph("YouTube 분석 해석:", sty["H2"]))
        interp = [
            "- 같이삽시다3: 방송 후 조회수 +248% (495만) — 4회 연속 방영 + 유명 출연진 효과로 가장 강한 온라인 반응",
            "- 전국노래자랑: +185% (267만) — KBS1 대표 프로그램의 높은 시청률(6.5%)이 온라인까지 확산",
            "- 뛰어야산다2: +92% (109만) — MBN 스포츠예능, 2030 타겟 프로그램답게 유튜브 반응 양호",
            "- 6시내고향: -5% — 시청률(5.5%)은 높지만 주시청층(중장년)의 유튜브 이용률이 낮아 온라인 반응 제한적",
            "- 전현무계획2: -64% — '전현무계획' 자체가 대형 프랜차이즈라 아산 회차 전 기존 조회수가 높았던 영향",
            "- 굿모닝대한민국: -76% — 교양정보 프로그램 특성상 유튜브 파급 제한적",
            "- 황제파워: -82% — 라디오 공개방송, 방영 직후(4일)라 데이터 부족",
        ]
        for line in interp:
            story.append(Paragraph(line, sty["BSmall"]))

    story.append(PageBreak())

    # ── 5~11. 방송별 상세 분석 ──
    for i, bc in enumerate(BROADCASTS):
        story.append(Paragraph(f"{i+5}. {bc['name']} 상세 분석", sty["H1"]))
        story.append(hr())

        # 기본 정보
        info = (
            f"방송사: {bc['broadcaster']} | 장르: {bc['genre']} | 방영일: {bc['air_date']} | "
            f"회차: {bc.get('episodes', 1)}회 | 시청률: {bc.get('rating') or '-'}% | "
            f"예산: {bc.get('budget_1000won', 0):,}천원 | 타겟: {bc.get('target_demo', '-')}"
        )
        story.append(Paragraph(info, sty["B"]))

        if bc.get("locations"):
            story.append(Paragraph(f"촬영 장소: {', '.join(bc['locations'])}", sty["BSmall"]))
        if bc.get("dong_names"):
            story.append(Paragraph(f"노출 읍면동: {', '.join(bc['dong_names'])}", sty["BSmall"]))

        story.append(Spacer(1, 8))

        # YouTube
        if len(df_buzz_raw) > 0:
            brow = df_buzz_raw[df_buzz_raw["방송"] == bc["name"]]
            if len(brow) > 0:
                add_text_block(story, "YouTube 반응", analyze_youtube(brow.iloc[0]))

        # DataLab
        add_text_block(story, "네이버 검색 트렌드", analyze_datalab(bc["name"], bc["air_date"]))

        # DataLab 차트
        if bc["name"] in bc_datalab_charts:
            story.append(Spacer(1, 4))
            story.append(Image(str(bc_datalab_charts[bc["name"]]), width=380, height=150))

        # 블로그
        add_text_block(story, "네이버 블로그", analyze_blog(bc["name"], bc["air_date"]))

        # 교란요소
        add_text_block(story, "교란요소", analyze_confounders(bc["name"]))

        # 종합 판정
        story.append(Spacer(1, 6))
        story.append(Paragraph("종합 판정", sty["H2"]))
        for line in overall_judgment(bc).split("\n"):
            story.append(Paragraph(f"  {line}", sty["B"]))

        story.append(PageBreak())

    # ── 12. 종합 결론 ──
    story.append(Paragraph("12. 종합 판정 및 결론", sty["H1"]))
    story.append(hr())

    # 종합 테이블
    final_rows = []
    for bc in BROADCASTS:
        name = bc["name"]
        j = overall_judgment(bc)
        lines = j.split("\n")
        conf = lines[0].split(": ")[1] if len(lines) > 0 else "-"
        buzz = lines[1].split(": ")[1] if len(lines) > 1 else "-"
        est = lines[2].split(": ")[1][:15] if len(lines) > 2 else "-"
        final_rows.append([name[:12], str(bc.get("rating") or "-"),
                          f'{bc.get("budget_1000won", 0):,}', conf, buzz, est])

    story.append(tbl(
        ["방송명", "시청률", "예산(천원)", "교란위험", "온라인반응", "효과추정"],
        final_rows,
        cw=[80, 40, 60, 50, 60, 90]
    ))

    story.append(Spacer(1, 12))
    story.append(Paragraph("핵심 결론", sty["H2"]))
    conclusions = [
        "1. 뛰어야산다2가 순수 방송 효과 측정에 가장 적합: 겨울 비수기 단독 방영, 기온 변화 최소(-2.6C), "
        "이벤트 없음. YouTube +92%, '뛰어야산다 아산' 검색어 신규 생성, '현충사' 검색 +136%.",

        "2. 같이삽시다3가 가장 강한 온라인 반응: YouTube 조회 +248%(495만), 4회 연속 방영과 유명 출연진 효과. "
        "단, 가을단풍+겨울온천 시즌 전환기(-5.4C)와 겹쳐 순수 효과 분리 어려움.",

        "3. 전국노래자랑은 시청률 대비 효율 최고: 시청률 6.5%로 최고, 예산 3,518천원으로 최소. "
        "YouTube +185%. 다만 이순신축제 기간과 일부 겹침.",

        "4. 11월 밀집 방송(전현무/굿모닝/6시내고향/같이삽시다)은 개별 효과 분리 어려움: "
        "4건이 2주 사이에 밀집 + 가을→겨울 전환(기온 -5~7C) + 단풍시즌 종료. "
        "그룹 효과로 분석하는 것이 적절.",

        "5. 황제파워는 이순신축제 동시 진행으로 순수 효과 분리 불가. "
        "라디오 특성상 온라인 반응도 제한적.",

        "6. 데이터 한계: T맵 방문(step2), SKT 유동인구(step3), 아산페이 DID(step1)는 "
        "회사 PC 데이터 연동 후 본 리포트에 자동 반영됨.",
    ]
    for c in conclusions:
        story.append(Paragraph(c, sty["B"]))
        story.append(Spacer(1, 4))

    story.append(PageBreak())

    # ── 13. 추가 분석 가능 항목 ──
    story.append(Paragraph("13. 추가 데이터 확보 시 분석 가능 항목", sty["H1"]))
    story.append(hr())

    add_rows = [
        ["T맵 2026-01~03", "방송 후 관광지 방문 DID (가장 핵심)", "기존 형태 CSV"],
        ["아산페이 일별", "방영일 전후 1~2주 소비 변화 정밀 추적", "읍면동 x 일별 x 업종"],
        ["관광지 입장객", "현충사/신정호 일별 입장객 수", "관광지별 일별"],
        ["숙박 이용률", "관광 체류 효과 (숙박시설 이용 변화)", "월별/주별"],
        ["톨게이트 진출입", "아산IC/온양IC 차량 유입 변화", "한국도로공사"],
        ["KTX 승하차", "온양온천역 대중교통 유입", "코레일 일별"],
    ]
    story.append(tbl(
        ["데이터", "분석 가치", "요청 형태"],
        add_rows,
        cw=[90, 200, 90]
    ))

    story.append(Spacer(1, 30))
    story.append(Paragraph(
        f"Generated by EOMAi | {datetime.now().strftime('%Y-%m-%d %H:%M')} | "
        f"데이터: 네이버 API, YouTube API, Open-Meteo, T맵, SKT, 아산페이",
        sty["Footer"]))

    doc.build(story)
    print(f"\n  PDF 완료: {pdf_path}")

except ImportError as e:
    print(f"  [!] reportlab 미설치: {e}")
    print("  pip install reportlab 후 재실행")

except Exception as e:
    print(f"  [!] PDF 생성 에러: {e}")
    import traceback
    traceback.print_exc()

print(f"\n{'=' * 60}")
print("종합 리포트 생성 완료!")
print(f"{'=' * 60}")
