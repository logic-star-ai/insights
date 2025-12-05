import traceback
import logging
import time
from typing import Optional

from tqdm import tqdm

from aitw.scrape.logging import setup_logging
from aitw.scrape.pr_classifier import PrClassifier
from aitw.database.pull_request_ingestor import BatchedPullRequestIngestor
from aitw.database.repository_ingestor import BatchedRepositoryIngestor
from aitw.database.pull_request import PullRequest
from aitw.database.repository import Repository
from aitw.database.connection import connect

from aitw.scrape.job import ScrapeJob, mark_job_done, mark_job_failed, pick_job
from aitw.scrape.scraper import GitHubScraper

def execute_job(job: ScrapeJob, token: str, db_conn: str):
    start_date = job.from_date
    end_date = job.to_date
    query = job.query

    logging.info(f'ℹ️  Executing job id={job.id} group={job.group} start={start_date} end={end_date} query={query}...')
        
    scraper = GitHubScraper(token, time_key=job.time_key)
            
    conn = connect(db_conn)
    pr_ingestor = BatchedPullRequestIngestor(conn, conn.cursor())
    repo_ingestor = BatchedRepositoryIngestor(conn, conn.cursor())
    
    seen = set()
    expected_total = scraper.count(start_date=start_date, end_date=end_date, filter=query)
    logging.info(f'ℹ️  Expecting to scrape {expected_total} pull requests')
    with tqdm(total=expected_total) as lbar: 
        for obj in scraper.scrape(start_date=start_date, end_date=end_date, filter=query):
            if isinstance(obj, PullRequest):
                if obj.id in seen:
                    continue
                
                PrClassifier.classify(obj)
                pr_ingestor.ingest(obj)
                
                seen.add(obj.id)
                lbar.update(1)
                
            if isinstance(obj, Repository):
                repo_ingestor.ingest(obj)
                
            lbar.update(0)
        
    if len(seen) != expected_total:
        logging.warning(f'⚠️  Worker has seen {len(seen)} but expected to see {expected_total} ({start_date=} {end_date=} {query=})')
    
    pr_ingestor.flush()
    repo_ingestor.flush()
    
    conn.close()
    
def worker(token, id, group, db_conn):
    print(f'Using GitHub token: {token}')
    if group is not None:
        print(f'Only working on jobs of group {group}')
    setup_logging(id)
    
    while True:
        job: Optional[ScrapeJob] = pick_job(db_conn, group)
        
        if job is None:
            logging.info('ℹ️  No jobs pending. Retry in 10s...')
            time.sleep(10)
            continue
        
        try:
            execute_job(job=job, token=token, db_conn=db_conn)
            mark_job_done(db_conn, job)
        except Exception as ex:
            mark_job_failed(db_conn, job)
            
            logging.error(f'❌ Exception while executing job={job}:')
            logging.exception(ex)
            traceback.print_exc()