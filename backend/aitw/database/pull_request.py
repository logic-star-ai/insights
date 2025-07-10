
from dataclasses import dataclass
from typing import List

@dataclass
class CommitAuthor:
    name: str
    email: str
    
@dataclass
class Commit:
    authors: List[CommitAuthor]
    
@dataclass
class Actor:    
    login: str
    type: str

@dataclass
class Comment:
    id: int
    created_at: str
    author: Actor | None # may got deleted
    body: str
    
@dataclass
class PullRequestFile:
    additions: int
    deletions: int
    path: str

@dataclass
class PullRequest:
    id: int
    url: str

    actor: Actor | None
    
    created_at: str
    closed_at: str
    isMerged: bool
    isDraft: bool
    
    title: str
    body: str
    
    additions: int
    deletions: int
    changed_files: int
    commits: int
    comments: int
    reviews: int
    
    base_ref: str
    head_ref: str
    
    base_repo_id: str
    head_repo_id: str
    
    files: List[PullRequestFile] | None
    commitsList: List[Commit] | None
    commentsList: List[Comment] | None
    
    agent: str | None = None
    primary_language: str | None = None
   