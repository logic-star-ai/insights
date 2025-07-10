
from aitw.insights.insight import Insight

# insight_language
# -----------------------------------------------------
# (Range) | Agent | Language | Total PRs | Merge Rate |
# -----------------------------------------------------

class BinnedPRInsight(Insight):
    
    def __init__(self, table_name, backend_conn, frontend_conn, key, bins, filter = None, order = None, remove_uncertain=True, joins=[]):
        super().__init__(table_name, backend_conn, frontend_conn, remove_uncertain)
        self.key = key
        self.bins = bins
        self.filter = filter
        self.order = order
        self.joins = joins
    
    def refresh(self):
        with self.frontend_conn.cursor() as frontend_cursor, self.backend_conn.cursor() as backend_cursor:
            frontend_cursor.execute(f"""
            CREATE TABLE {self.staging_table_name} (
                agent  text,
                bin    text,
                filter text,
                
                min    float8,
                
                total_prs  int8,
                closed_prs int8,
                merged_prs int8,
                
                merge_rate float8,
                merge_rate_lb float8,
                merge_rate_ub float8,
                
                total_pr_share float8,
                
                PRIMARY KEY (agent, bin, filter)
            );               
            """)
            
            for filter_name, filter_where in [('all', 'TRUE'), ('popular', 'stars > 10')]:
                query = f"""
                SELECT 
                    prs.agent,
                    {
                        f'''
                        CASE
                        {' '.join([f"WHEN {self.key} <= {threshold} THEN '{name}'::text" for threshold, name in self.bins[:-1]])}
                        ELSE '{self.bins[-1][1]}'::text
                        END AS bin
                        '''
                        if isinstance(self.bins, list)
                        else f'{self.bins} AS bin'
                    },
                    MIN({f'{self.key}' if self.order is None else self.order}) as min,
                    COUNT(*) total_prs,
                    COUNT(*) FILTER (WHERE closed_at IS NOT NULL) closed_prs,
                    COUNT(*) FILTER (WHERE merged = True) merged_prs
                FROM
                    prs
                    JOIN repos ON prs.base_repo_id = repos.id
                    {' '.join([j for j in self.joins])}
                WHERE {f'{self.filter}' if self.filter else 'TRUE' } AND {filter_where}
                GROUP BY agent, bin
                HAVING agent IS NOT NULL
                ORDER BY min
                """
                backend_cursor.execute(query)
                
                for agent, popularity_bin, popularity_min, total_prs, closed_prs, merged_prs in backend_cursor.fetchall():
                    merge_rate, merge_rate_lb, merge_rate_ub = self.merge_rate(closed_prs, merged_prs)
                    
                    frontend_cursor.execute(
                        f"""INSERT INTO {self.staging_table_name} 
                        (agent, bin, filter, min, total_prs, closed_prs, merged_prs, merge_rate, merge_rate_lb, merge_rate_ub) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
                        (agent, popularity_bin, filter_name, popularity_min, total_prs, closed_prs, merged_prs, merge_rate, merge_rate_lb, merge_rate_ub)
                    )
                    
                frontend_cursor.execute(f"""
                UPDATE {self.staging_table_name} i SET 
                    total_pr_share = 100*total_prs::DOUBLE PRECISION/
                    (SELECT SUM(total_prs) FROM {self.staging_table_name} p WHERE p.agent = i.agent and p.filter = i.filter)               
                """)

            self.swap_tables()
