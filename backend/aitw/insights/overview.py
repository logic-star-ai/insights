
from aitw.insights.insight import Insight
from statsmodels.stats.proportion import proportion_confint

class OverviewInsight(Insight):
    
    def __init__(self, backend_conninfo, frontend_conninfo):
        super().__init__("insight_overview", backend_conninfo, frontend_conninfo)
    
    def refresh(self):
        with self.frontend_conn.cursor() as frontend_cursor, self.backend_conn.cursor() as backend_cursor:
            frontend_cursor.execute(f"""
            CREATE TABLE {self.staging_table_name} (
                agent text,
                total_prs int8,
                closed_prs int8,
                merged_prs int8,
                merge_rate float8,
                merge_rate_lb float8,
                merge_rate_ub float8,
                PRIMARY KEY (agent)
            );               
            """)
            
            backend_cursor.execute("""
            SELECT agent,
                count(*) AS total_prs,
                count(*) FILTER (WHERE closed_at IS NOT NULL) AS closed_prs,
                count(*) FILTER (WHERE merged = true) AS merged_prs
            FROM prs
            WHERE created_at >= '2025-05-15 00:00:00'::timestamp without time zone 
            AND agent is NOT NULL
            GROUP BY agent
            """)
            
            for agent, total_prs, closed_prs, merged_prs in backend_cursor.fetchall():
                merge_rate = merged_prs/closed_prs if closed_prs > 0 else None
                merge_rate_lb, merge_rate_ub = proportion_confint(merged_prs, closed_prs, alpha=0.05, method='beta') if closed_prs > 0 else (None, None)
                
                frontend_cursor.execute(
                    f"""INSERT INTO {self.staging_table_name} 
                    (agent, total_prs, closed_prs, merged_prs, merge_rate, merge_rate_lb, merge_rate_ub) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)""", 
                    (agent, total_prs, closed_prs, merged_prs, merge_rate, merge_rate_lb, merge_rate_ub)
                )

            self.swap_tables()