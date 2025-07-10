from dataclasses import asdict
import json
import logging
from typing import List

from dacite import from_dict

from aitw.database.pull_request import Actor, PullRequest, PullRequestFile, Commit, Comment


class BatchedPullRequestIngestor:
    select_fields = ['id', 'agent', 'url', 'title', 'description', 'created_at', 'closed_at', 'merged', 'is_draft', 'additions', 'deletions', 'changed_files', 'comments', 'commits', 'reviewers', 'base_repo_id', 'head_repo_id', 'base_ref', 'head_ref', 'author_login', 'author_type', 'files', 'commits_list', 'comments_list', 'primary_language']
    
    def __init__(self, conn, cursor, batch_size=100, auto_commit=True):
        self.conn = conn
        self.cursor = cursor
        self.batch_size = batch_size
        self.buffer = []
        self.auto_commit = auto_commit
        
    @staticmethod
    def row_to_pr(row: List):
        return PullRequest(
            id = row[0],
            agent = row[1],
            url = row[2],
            title = row[3],
            body = row[4],
            created_at = row[5],
            closed_at = row[6],
            isMerged = row[7],
            isDraft = row[8],
            additions= row[9],
            deletions= row[10],
            changed_files= row[11], 
            comments = row[12], 
            commits = row[13], 
            reviews = row[14], 
            base_repo_id = row[15], 
            head_repo_id = row[16], 
            base_ref = row[17], 
            head_ref = row[18], 
            actor = Actor(row[19], row[20]),
            files=[from_dict(PullRequestFile, x) for x in row[21]] if row[21] is not None else None,
            commitsList=[from_dict(Commit, x) for x in row[22]] if row[22] is not None else None,
            commentsList=[from_dict(Comment, x) for x in row[23]] if row[23] is not None else None,
            primary_language=row[24]
        )
    
    @staticmethod
    def pr_to_row(pr: PullRequest):
        return (
            pr.id,
            pr.agent,
            pr.url,
            pr.title,
            pr.body,
            pr.created_at,
            pr.closed_at,
            pr.isMerged,
            pr.isDraft,
            pr.additions,
            pr.deletions,
            pr.changed_files,
            pr.comments,
            pr.commits,
            pr.reviews,
            pr.base_repo_id,
            pr.head_repo_id,
            pr.base_ref,
            pr.head_ref,
            pr.actor.login if pr.actor is not None else None,
            pr.actor.type if pr.actor is not None else None,
            json.dumps([asdict(c) for c in pr.files]) if pr.files is not None else None,
            json.dumps([asdict(c) for c in pr.commitsList]) if pr.commitsList is not None else None,
            json.dumps([asdict(c) for c in pr.commentsList]) if pr.commentsList is not None else None,
            pr.primary_language
        )

    def flush(self):
        self.buffer.sort(key=lambda row: row[0])
        self.cursor.executemany(
        f"""
        INSERT INTO prs ({', '.join([f for f in self.select_fields])})
        VALUES ({', '.join(['%s' for _ in range(len(self.select_fields))])})
        ON CONFLICT (id)
        DO UPDATE SET {',\n'.join([f'{f} = EXCLUDED.{f}' for f in self.select_fields])}
        """,
            self.buffer,
        )
        
        if self.auto_commit:
            self.conn.commit()

        logging.info(f"📊 Ingested {len(self.buffer)} pull requests into db.")
        self.buffer = []

    def ingest(self, pr: PullRequest):
        self.buffer.append(BatchedPullRequestIngestor.pr_to_row(pr))

        if len(self.buffer) >= self.batch_size:
            self.flush()
