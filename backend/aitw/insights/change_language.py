from aitw.insights.binned import BinnedPRInsight

class ChangeLanguageInsight(BinnedPRInsight):
    languages = [
        "Python",
        "TypeScript",
        "JavaScript",
        "HTML",
        "Java",
        "YAML",
        "Go",
        "JSON",
        "Rust",
        "C#",
        "C++",
        "PHP",
        "Text"
    ]

    def __init__(self, backend_conninfo, frontend_conninfo):
        super().__init__(
            "insight_change_language",
            backend_conninfo,
            frontend_conninfo,
            key="prs.primary_language",
            bins=f"""
            CASE 
                WHEN prs.primary_language IN ({",".join(f"'{lang}'" for lang in self.languages)}) 
                THEN prs.primary_language
                ELSE 'Other'
            END 
            """,
            order=f"""
            CASE 
                {" ".join([f"WHEN prs.primary_language = '{name}' THEN {idx}" for idx, name in enumerate(self.languages)])}
                ELSE 1000
            END 
            """,
            remove_uncertain=False
        )