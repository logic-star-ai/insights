from aitw.database.pull_request import PullRequest

class PrClassifier:
    @staticmethod
    def classify(pr: PullRequest):
        branch = pr.head_ref
        
        if branch.startswith('codex/'):
            pr.agent = "codex"
        elif branch.startswith('copilot/'):
            pr.agent = "copilot"
        elif branch.startswith('cursor/'):
            pr.agent = "cursor"
        elif branch.startswith('claude/'):
            pr.agent = "claude"
            
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
            if 'openhands-agent' in first_commit_first_authors:
                pr.agent = 'openhands'
                
        if pr.agent is None and pr.actor and pr.actor.type == 'Bot':
            pr.agent = "bot"

        if pr.agent is None:
            pr.agent = "human"
            
        return pr