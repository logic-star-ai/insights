from typing import Optional, Tuple
from statsmodels.stats.proportion import proportion_confint

class Insight:
    def __init__(self, table_name, backend_conn, frontend_conn, remove_uncertain=True):
        self.table_name = table_name
        self.staging_table_name = f"staging_{table_name}"
        self.old_table_name = f"old_{table_name}"
        
        self.remove_uncertain = remove_uncertain

        self.backend_conn = backend_conn
        self.frontend_conn = frontend_conn

    def merge_rate(
        self, closed: Optional[int], merged: Optional[int]
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        if closed is None or closed == 0:
            return None, None, None
        
        if merged is None:
            merged = 0
            
        merge_rate = merged / closed
        merge_rate_lb, merge_rate_ub = proportion_confint(
            merged, closed, alpha=0.05, method="beta"
        )
        
        # Remove uncertain data points
        if merge_rate_ub - merge_rate_lb > 0.5 and self.remove_uncertain:
            return None, None, None
        
        return merge_rate, merge_rate_lb, merge_rate_ub

    def swap_tables(self):
        with self.frontend_conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                id SERIAL PRIMARY KEY
            );""")
            cursor.execute(
                f"ALTER TABLE {self.table_name} RENAME TO {self.old_table_name};"
            )
            cursor.execute(
                f"ALTER TABLE {self.staging_table_name} RENAME TO {self.table_name};"
            )
            cursor.execute(f"DROP TABLE {self.old_table_name};")

    def refresh(self):
        pass
