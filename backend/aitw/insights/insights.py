from typing import Dict
from aitw.database.connection import connect
from aitw.insights.binned import BinnedPRInsight
from aitw.insights.language import LanguageInsight
from aitw.insights.overview import OverviewInsight
from aitw.insights.insight import Insight
from aitw.insights.change_language import ChangeLanguageInsight

def insights(insight, backend_conninfo, frontend_conninfo):
    frontend_conn = connect(frontend_conninfo)
    backend_conn = connect(backend_conninfo)
    
    hourly_insights: Dict[str, Insight] = {
        "overview": OverviewInsight(backend_conn, frontend_conn),
        "daily_trend": BinnedPRInsight(
            "insight_daily",
            backend_conn,
            frontend_conn,
            key="(EXTRACT(EPOCH FROM prs.created_at)::int)",
            bins="(to_char(prs.created_at, 'YYYY-MM-DD'::text))",
            filter="created_at >= '2025-05-15 00:00:00'::timestamp without time zone",
        ),
    }
    
    daily_insights: Dict[str, Insight] = {
        "language": LanguageInsight(backend_conn, frontend_conn),
        "repo_popularity": BinnedPRInsight(
            "insight_repo_popularity",
            backend_conn,
            frontend_conn,
            key="repos.stars",
            bins=[
                (9, "<10"),
                (20, "10-20"),
                (100, "21-100"),
                (1000, "101-1000"),
                (None, "1000+"),
            ],
        ),
        "change_complexity": BinnedPRInsight(
            "insight_change_complexity",
            backend_conn,
            frontend_conn,
            key="prs.additions + prs.deletions",
            bins=[
                (2, "1-2"),
                (10, "2-10"),
                (30, "11-30"),
                (100, "31-100"),
                (300, "101-300"),
                (1000, "501-1000"),
                (None, "1000+"),
            ],
        ),
        "changed_files": BinnedPRInsight(
            "insight_changed_files",
            backend_conn,
            frontend_conn,
            key="prs.changed_files",
            bins=[
                (0, "0"),
                (1, "1"),
                (2, "2"),
                (3, "3"),
                (4, "4"),
                (5, "5"),
                (6, "6"),
                (7, "7"),
                (8, "8"),
                (9, "9"),
                (None, "10+"),
            ],
        ),
        "ad_ratio": BinnedPRInsight(
            "insight_ad_ratio",
            backend_conn,
            frontend_conn,
            key="""
            CASE
                WHEN prs.deletions + prs.additions = 0 THEN NULL
                ELSE prs.additions::double precision / (prs.deletions + prs.additions)
            END
            """,
            bins=[
                (0.1, "0.0-0.1"),
                (0.2, "0.1-0.2"),
                (0.3, "0.2-0.3"),
                (0.4, "0.3-0.4"),
                (0.5, "0.4-0.5"),
                (0.6, "0.5-0.6"),
                (0.7, "0.6-0.7"),
                (0.8, "0.7-0.8"),
                (0.9, "0.8-0.9"),
                (None, "0.9-1.0"),
            ],
            filter="prs.deletions + prs.additions > 0",
        ),
        "time_to_close": BinnedPRInsight(
            "insight_time_to_close",
            backend_conn,
            frontend_conn,
            key="EXTRACT(EPOCH FROM(closed_at - created_at))",
            bins=[
                (60, "< 1m"),
                (10 * 60, "1-10m"),
                (60 * 60, "< 1h"),
                (60 * 60 * 24, "< 1d"),
                (None, "> 1d"),
            ],
        ),
        # "change_type": BinnedPRInsight(
        #     "insight_change_type",
        #     backend_conn,
        #     frontend_conn,
        #     key="prs_toc.type",
        #     bins="prs_toc.type",
        #     order="1",
        #     joins=["JOIN prs_toc ON prs.id = prs_toc.pr_id"],
        #     remove_uncertain=False,
        # ),
        "change_language": ChangeLanguageInsight(backend_conn, frontend_conn)
    }
    
    all_insights: Dict[str, Insight] = {**hourly_insights, **daily_insights}

    if insight == "hourly":
        for name, insight in hourly_insights.items():
            print(f"⏳ Updating insight {name}")
            insight.refresh()
            print(f"✅ Updated insight {name}")
    elif insight == "daily":
        for name, insight in daily_insights.items():
            print(f"⏳ Updating insight {name}")
            insight.refresh()
            frontend_conn.commit()
            print(f"✅ Updated insight {name}")
    elif insight in all_insights:
        print(f"⏳ Updating insight {insight}")
        all_insights[insight].refresh()
        print(f"✅ Updated insight {insight}")
    else:
        print(f"❌ Invalid insight name: hourly|daily|{'|'.join(all_insights.keys())}")

    # Update last_updated
    frontend_conn.execute(
        "UPDATE metadata SET value = to_jsonb(((NOW() AT TIME ZONE 'UTC')::DATE)::text) WHERE key='last_updated';"
    )
    frontend_conn.commit()
    frontend_conn.close()
