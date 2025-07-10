from collections import defaultdict
from typing import Dict
from tqdm import tqdm
from aitw.database.pull_request import PullRequest
from aitw.database.connection import connect
from aitw.database.pull_request_ingestor import BatchedPullRequestIngestor

class PrClassifier:
    endings = {
        "py": "Python",
        "ts": "TypeScript",
        "tsx": "TypeScript",
        "js": "JavaScript",
        "jsx": "JavaScript",
        "html": "HTML",
        "css": "HTML",
        "rs": "Rust",
        "java": "Java",
        "cs": "C#",
        "cpp": "C++",
        "hpp": "C++",
        "c": "C",
        "h": "C",
        "php": "PHP",
        "yaml": "YAML",
        "yml": "YAML",
        "go": "Go",
        "json": "JSON",
        "md": "Markdown",
        "txt": "Text"
    }
    
    @staticmethod
    def classify_primary_language(pr: PullRequest):
        if not pr.files:
            pr.primary_language = None
            return
        
        result: Dict[str, int] = defaultdict(int)
        for path, changes in [(f.path, f.additions + f.deletions) for f in pr.files]:
            ending = path.split(".")[-1]
            if path.endswith('package.json') or path.endswith('package-lock.json'):
                # or ending == 'lock' or ending == 'json' or ending == 'yaml':
                continue
                
            if ending in PrClassifier.endings:
                result[PrClassifier.endings[ending]] += changes
            else:
                result[ending] += changes
        
        lang = None
        if len(result.items()) > 0:
            lang, changes = sorted(result.items(), key=lambda x: x[1])[-1]
        
        pr.primary_language = lang
        
    @staticmethod
    def classify_agent(pr: PullRequest):
        branch = pr.head_ref
        
        if branch.startswith('codex/'):
            pr.agent = "codex"
        elif branch.startswith('copilot/'):
            pr.agent = "copilot"
        elif branch.startswith('cursor/'):
            pr.agent = "cursor"
        elif branch.startswith('claude/'):
            pr.agent = "claude"
        elif branch.startswith('cosine/'):
            pr.agent = "cosine"
            
        # By author
        elif pr.actor is not None and pr.actor.login == 'devin-ai-integration':
            pr.agent = "devin"
        elif pr.actor is not None and pr.actor.login == 'codegen-sh':
            pr.agent = "codegen"
        elif pr.actor is not None and pr.actor.login == 'tembo-io':
            pr.agent = "tembo"
            
        # By deep inspection
        if pr.commitsList and len(pr.commitsList) >= 1:
            first_commit_first_authors = [a.name for a in pr.commitsList[0].authors]
            if 'google-labs-jules[bot]' in first_commit_first_authors:
                pr.agent = 'jules'
            if 'claude[bot]' in first_commit_first_authors:
                pr.agent = 'claude'
            if 'openhands' in first_commit_first_authors:
                pr.agent = 'openhands'
                
        if pr.agent is None and pr.actor and pr.actor.type == 'Bot':
            pr.agent = "bot"

        if pr.agent is None:
            pr.agent = "human"
    
    @staticmethod
    def classify(pr: PullRequest):
        PrClassifier.classify_primary_language(pr)
        PrClassifier.classify_agent(pr)
            
        return pr
    
def reclassify(dbconn_info):
    conn = connect(dbconn_info)
    classifier = PrClassifier()
    
    with conn.cursor(name='streaming_all_reclassify') as streaming_cursor, conn.cursor() as ingestor_cursor:        
        print('Estimating total number of prs to analyze...')
        streaming_cursor.execute("SELECT reltuples::BIGINT AS estimate FROM pg_class WHERE relname = 'prs';")
        
        (total_count, ) = streaming_cursor.fetchone()
        
        with tqdm(total=total_count, smoothing=0.0) as pbar:
            ingestor = BatchedPullRequestIngestor(conn, ingestor_cursor, auto_commit=False, batch_size=100000)
            
            # Stream all prs and reclassify
            streaming_cursor.execute(f"SELECT {', '.join(BatchedPullRequestIngestor.select_fields)} FROM prs ORDER BY id FOR UPDATE")
            streaming_cursor.itersize = 50000
            for row in streaming_cursor:
                pr = BatchedPullRequestIngestor.row_to_pr(row)
                classifier.classify(pr)
                ingestor.ingest(pr)
                pbar.update(1)
    
    conn.commit()
    conn.close()