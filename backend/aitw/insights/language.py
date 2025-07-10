from aitw.insights.binned import BinnedPRInsight


class LanguageInsight(BinnedPRInsight):
    languages = [
        "Python",
        "TypeScript",
        "JavaScript",
        "HTML",
        "Java",
        "Go",
        "C#",
        "C++",
        "Rust",
        "PHP",
        # "Ruby",
        "Shell",
        "C",
        "Kotlin",
    ]

    def __init__(self, backend_conninfo, frontend_conninfo):
        super().__init__(
            "insight_language",
            backend_conninfo,
            frontend_conninfo,
            key="repos.primary_language",
            bins=f"""
            CASE 
                WHEN repos.primary_language IN ({",".join(f"'{lang}'" for lang in self.languages)}) 
                THEN repos.primary_language
                ELSE 'Other'
            END 
            """,
            order=f"""
            CASE 
                {" ".join([f"WHEN repos.primary_language = '{name}' THEN {idx}" for idx, name in enumerate(self.languages)])}
                ELSE 1000
            END 
            """,
            remove_uncertain=False,
        )
