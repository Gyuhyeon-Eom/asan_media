"""아산시청 방송 홍보효과 분석기 — 메인 오케스트레이터.

아산시청 제작/협찬 방송(TV 출연 + 자체 SNS 콘텐츠) 후
관광 관련 크롤링 지표 변화를 종합 측정.

측정 지표:
  1. 검색량 변화 — 네이버 데이터랩 (아산 관광지/맛집/숙박 키워드)
  2. 콘텐츠 증감 — 블로그/뉴스/카페/유튜브/인스타 게시물 수
  3. 후기 증감 — 실제 방문 후기 게시물 변화
  4. 인게이지먼트 — 조회수/좋아요/댓글 총량

Usage:
    from asan_broadcast_impact import AsanBroadcastAnalyzer
    from config import BroadcastInfo

    broadcast = BroadcastInfo(
        title="1박2일 아산편",
        broadcast_date="2026-04-10",
        broadcast_type="tv",
        channel="KBS",
        featured_spots=["외암민속마을", "온양온천"],
    )

    analyzer = AsanBroadcastAnalyzer()
    report = analyzer.analyze(broadcast)
    report.summary()
    report.to_csv()
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

from collectors import (
    NaverSearchCollector,
    NaverDatalabCollector,
    YouTubeCollector,
    InstagramCollector,
)
from config import (
    BroadcastInfo, CrawlConfig, TourismCategory,
    CORE_KEYWORDS, CATEGORY_KEYWORD_MAP, ALL_KEYWORDS,
)
from models import CollectionResult, Platform
from utils import (
    setup_logging,
    items_to_dataframe,
    trends_to_dataframe,
    compute_broadcast_impact,
)

load_dotenv()


@dataclass
class AsanAnalysisReport:
    """아산 방송 효과 분석 리포트."""

    broadcast: BroadcastInfo
    results: list[CollectionResult] = field(default_factory=list)
    search_trends: dict = field(default_factory=dict)   # 네이버 데이터랩 결과
    impact: dict = field(default_factory=dict)

    @property
    def items_df(self) -> pd.DataFrame:
        all_items = []
        for r in self.results:
            all_items.extend(r.items)
        return items_to_dataframe(all_items)

    @property
    def trends_df(self) -> pd.DataFrame:
        all_trends = []
        for r in self.results:
            all_trends.extend(r.trends)
        return trends_to_dataframe(all_trends)

    def to_csv(self, output_dir: str = "output") -> None:
        """분석 결과 CSV 내보내기."""
        Path(output_dir).mkdir(exist_ok=True)
        slug = self.broadcast.title.replace(" ", "_")[:20]
        date = self.broadcast.broadcast_date

        df = self.items_df
        if not df.empty:
            path = f"{output_dir}/{date}_{slug}_콘텐츠.csv"
            df.to_csv(path, index=False, encoding="utf-8-sig")
            logging.getLogger().info(f"저장: {path} ({len(df)}건)")

        tdf = self.trends_df
        if not tdf.empty:
            path = f"{output_dir}/{date}_{slug}_트렌드.csv"
            tdf.to_csv(path, index=False, encoding="utf-8-sig")

    def summary(self) -> str:
        """분석 결과 요약 출력."""
        b = self.broadcast
        lines = [
            f"\n{'='*65}",
            f"  📡 아산시청 방송 홍보효과 분석 리포트",
            f"{'='*65}",
            f"  방송명     : {b.title}",
            f"  방송일     : {b.broadcast_date}",
            f"  방송유형   : {b.broadcast_type} ({b.channel})",
            f"  노출 관광지: {', '.join(b.featured_spots) or '미지정'}",
            f"{'─'*65}",
        ]

        # ── 플랫폼별 수집 현황 ──
        lines.append("  📊 플랫폼별 수집 현황")
        for r in self.results:
            status = "✅" if r.success else "❌"
            eng = f"{r.total_engagement:>12,}" if r.items else "      (트렌드)"
            lines.append(
                f"    {status} {r.platform.value:18s} │ {len(r.items):>5}건 │ 인게이지먼트 {eng}"
            )

        # ── 검색 트렌드 변화 ──
        if self.search_trends:
            lines.append(f"\n{'─'*65}")
            lines.append("  🔍 검색 트렌드 변화 (네이버 데이터랩)")
            lines.append(f"    {'키워드':20s} │ {'방송전':>8s} │ {'방송후':>8s} │ {'변화율':>10s}")
            lines.append(f"    {'─'*52}")
            for kw, data in self.search_trends.items():
                ba = data.get("before_avg", 0)
                aa = data.get("after_avg", 0)
                lift = data.get("lift_pct", 0)
                l_str = f"+{lift}%" if lift != float("inf") else "∞"
                lines.append(f"    {kw:20s} │ {ba:>8.1f} │ {aa:>8.1f} │ {l_str:>10s}")

        # ── 전후 비교 ──
        impact = self.impact
        if impact and "error" not in impact:
            lines.append(f"\n{'─'*65}")
            lines.append("  📈 방송 전후 콘텐츠 지표 비교")

            overall = impact.get("overall", {})
            before = overall.get("before", {})
            after = overall.get("after", {})
            lift = overall.get("lift_pct", {})

            metrics = [
                ("게시물 수", "post_count"),
                ("조회수", "view_count"),
                ("좋아요", "like_count"),
                ("댓글", "comment_count"),
            ]
            lines.append(f"    {'지표':12s} │ {'방송 전':>10s} │ {'방송 후':>10s} │ {'변화율':>10s}")
            lines.append(f"    {'─'*48}")
            for label, key in metrics:
                bv = before.get(key, 0)
                av = after.get(key, 0)
                lv = lift.get(key, 0)
                l_str = f"+{lv}%" if lv != float("inf") else "∞"
                lines.append(f"    {label:12s} │ {bv:>10,} │ {av:>10,} │ {l_str:>10s}")

            # 카테고리별
            by_cat = impact.get("by_category", {})
            if by_cat:
                lines.append(f"\n  🏷️  관광 카테고리별 게시물 변화")
                for cat, data in by_cat.items():
                    b_cnt = data["before"]["post_count"]
                    a_cnt = data["after"]["post_count"]
                    l_pct = data["lift_pct"]["post_count"]
                    l_str = f"+{l_pct}%" if l_pct != float("inf") else "∞"
                    lines.append(f"    {cat:10s} │ {b_cnt:>5} → {a_cnt:>5}건 ({l_str})")

            # 관광지 언급 순위
            top_spots = impact.get("top_spots", {})
            if top_spots:
                lines.append(f"\n  📍 방송 후 가장 많이 언급된 관광지")
                for i, (spot, cnt) in enumerate(list(top_spots.items())[:10], 1):
                    lines.append(f"    {i:>2}. {spot} ({cnt}건)")

            # 후기 증가율
            review_surge = impact.get("review_surge_pct", 0)
            if review_surge:
                lines.append(f"\n    📝 방문 후기 증가율: +{review_surge}%")

            peak = impact.get("peak_date")
            if peak:
                lines.append(f"    🔥 피크 날짜: {peak}")

        lines.append(f"\n{'='*65}\n")

        text = "\n".join(lines)
        print(text)
        return text


class AsanBroadcastAnalyzer:
    """아산시청 방송 홍보효과 분석기.

    방송 건별로 다음을 수행:
    1. 네이버 데이터랩으로 검색 트렌드 변화 측정
    2. 네이버 블로그/뉴스/카페에서 관련 콘텐츠 수집 + 관광 카테고리 분류
    3. YouTube에서 관련 영상 수집 + 인게이지먼트
    4. Instagram에서 해시태그 기반 게시물 수집
    5. 방송 전후 지표 비교 (전체/플랫폼별/카테고리별)
    """

    def __init__(self, config: Optional[CrawlConfig] = None, log_level: int = logging.INFO):
        setup_logging(log_level)
        self.config = config or CrawlConfig()
        self.logger = logging.getLogger(self.__class__.__name__)

        self.naver_search = NaverSearchCollector()
        self.naver_datalab = NaverDatalabCollector()
        self.youtube = YouTubeCollector()
        self.instagram = InstagramCollector()

    def analyze(
        self,
        broadcast: BroadcastInfo,
        config: Optional[CrawlConfig] = None,
    ) -> AsanAnalysisReport:
        """단일 방송 건 분석."""
        cfg = config or self.config
        bd = datetime.strptime(broadcast.broadcast_date, "%Y-%m-%d")
        start = bd - timedelta(days=cfg.days_before)
        end = bd + timedelta(days=cfg.days_after)

        # 분석 키워드 결정
        keywords = self._build_keywords(broadcast, cfg)
        self.logger.info(
            f"분석 시작: '{broadcast.title}' ({broadcast.broadcast_date}) | "
            f"키워드 {len(keywords)}개 | 기간 {start:%m/%d}~{end:%m/%d}"
        )

        results: list[CollectionResult] = []

        # ── 1. 네이버 데이터랩 검색 트렌드 ──
        search_trends = {}
        if "naver_datalab" in cfg.platforms:
            self.logger.info("▶ 네이버 데이터랩 검색 트렌드 수집...")
            # 코어 키워드 + 방송 노출 관광지 키워드 (최대 5개씩 배치)
            trend_keywords = list(dict.fromkeys(
                CORE_KEYWORDS[:3] + broadcast.featured_spots[:2]
            ))[:5]
            search_trends = self.naver_datalab.get_broadcast_impact_trend(
                keywords=trend_keywords,
                broadcast_date=bd,
                days_before=cfg.days_before,
                days_after=cfg.days_after,
            )
            # 트렌드 데이터도 results에 추가
            for kw_trends in search_trends.values():
                trend_data = kw_trends.get("trend_data", [])
                if trend_data:
                    results.append(CollectionResult(
                        platform=Platform.NAVER_DATALAB,
                        keyword=trend_data[0].keyword,
                        trends=trend_data,
                        total_count=len(trend_data),
                    ))

        # ── 2. 네이버 블로그/뉴스/카페 ──
        naver_platforms_in_cfg = [
            p for p in cfg.platforms
            if p in ("naver_blog", "naver_news", "naver_cafe")
        ]
        if naver_platforms_in_cfg:
            search_type_map = {
                "naver_blog": Platform.NAVER_BLOG,
                "naver_news": Platform.NAVER_NEWS,
                "naver_cafe": Platform.NAVER_CAFE,
            }
            search_types = [search_type_map[p] for p in naver_platforms_in_cfg]

            for kw in keywords:
                self.logger.info(f"▶ 네이버 수집: '{kw}'")
                r = self.naver_search.safe_collect(
                    keyword=kw,
                    start_date=start,
                    end_date=end,
                    max_items=cfg.max_items_per_platform,
                    search_types=search_types,
                )
                results.append(r)

        # ── 3. YouTube ──
        if "youtube" in cfg.platforms:
            for kw in keywords:
                self.logger.info(f"▶ YouTube 수집: '{kw}'")
                r = self.youtube.safe_collect(
                    keyword=kw,
                    start_date=start,
                    end_date=end,
                    max_items=min(cfg.max_items_per_platform, 50),
                )
                results.append(r)

        # ── 4. Instagram ──
        if "instagram" in cfg.platforms:
            ig_tags = self._build_instagram_tags(broadcast)
            for tag in ig_tags:
                self.logger.info(f"▶ Instagram 수집: '#{tag}'")
                r = self.instagram.safe_collect(
                    keyword=tag,
                    start_date=start,
                    end_date=end,
                    max_items=min(cfg.max_items_per_platform, 50),
                )
                results.append(r)

        # ── 5. 전후 비교 분석 ──
        all_items = []
        for r in results:
            all_items.extend(r.items)

        items_df = items_to_dataframe(all_items)
        impact = compute_broadcast_impact(items_df, bd, cfg.days_before, cfg.days_after)

        report = AsanAnalysisReport(
            broadcast=broadcast,
            results=results,
            search_trends=search_trends,
            impact=impact,
        )

        self.logger.info(f"분석 완료: 총 {len(all_items)}건 수집")
        return report

    def compare_broadcasts(
        self,
        broadcasts: list[BroadcastInfo],
        config: Optional[CrawlConfig] = None,
    ) -> pd.DataFrame:
        """여러 방송 건의 효과 비교표 생성.

        Usage:
            broadcasts = [
                BroadcastInfo(title="1박2일 아산편", broadcast_date="2026-04-10", ...),
                BroadcastInfo(title="아산시 유튜브 온천 영상", broadcast_date="2026-03-15", ...),
            ]
            comparison = analyzer.compare_broadcasts(broadcasts)
        """
        rows = []
        for bc in broadcasts:
            self.logger.info(f"\n{'='*40}\n방송 비교 분석: {bc.title}\n{'='*40}")
            report = self.analyze(bc, config)

            overall = report.impact.get("overall", {})
            after = overall.get("after", {})
            lift = overall.get("lift_pct", {})

            rows.append({
                "방송명": bc.title,
                "방송일": bc.broadcast_date,
                "유형": bc.broadcast_type,
                "채널": bc.channel,
                "방송후_게시물수": after.get("post_count", 0),
                "방송후_조회수": after.get("view_count", 0),
                "방송후_인게이지먼트": after.get("engagement_score", 0),
                "게시물_증가율": lift.get("post_count", 0),
                "조회수_증가율": lift.get("view_count", 0),
                "후기_증가율": report.impact.get("review_surge_pct", 0),
                "피크날짜": report.impact.get("peak_date", ""),
            })

        return pd.DataFrame(rows)

    def _build_keywords(self, broadcast: BroadcastInfo, cfg: CrawlConfig) -> list[str]:
        """방송 건에 맞는 수집 키워드 생성."""
        keywords = []

        # 코어 키워드
        if cfg.use_core_keywords:
            keywords.extend(CORE_KEYWORDS)

        # 카테고리별 키워드
        if cfg.use_category_keywords:
            for cat in cfg.target_categories:
                cat_kws = CATEGORY_KEYWORD_MAP.get(cat, [])
                keywords.extend(cat_kws[:3])  # 카테고리당 상위 3개

        # 방송에 노출된 관광지 키워드
        for spot in broadcast.featured_spots:
            keywords.append(spot)
            keywords.append(f"아산 {spot}")

        # 방송 프로그램 + 아산 조합
        if broadcast.title:
            keywords.append(f"{broadcast.title} 아산")

        # 방송 건 커스텀 키워드
        keywords.extend(broadcast.custom_keywords)

        # 중복 제거 + 순서 유지
        return list(dict.fromkeys(keywords))

    def _build_instagram_tags(self, broadcast: BroadcastInfo) -> list[str]:
        """Instagram 해시태그 목록 생성."""
        tags = ["아산", "아산여행", "아산관광", "아산맛집"]

        for spot in broadcast.featured_spots:
            tag = spot.replace(" ", "")
            tags.append(tag)

        if broadcast.title:
            tags.append(broadcast.title.replace(" ", ""))

        return list(dict.fromkeys(tags))[:8]  # 최대 8개 (rate limit 고려)
