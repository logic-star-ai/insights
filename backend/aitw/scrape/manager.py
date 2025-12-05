from datetime import datetime, timedelta, timezone
import time
import click
from tqdm import tqdm

from aitw.database.connection import connect
from aitw.scrape.job import CreateScrapeJob, JobManager

def slice(start, end, slice_size):
    total_seconds = (end - start).total_seconds()

    slices = int(total_seconds) // slice_size

    slices_arr = []
    for i in range(slices):
        slice_start = start + timedelta(seconds=i * slice_size)
        slice_end = start + timedelta(seconds=(i + 1) * slice_size)
        slices_arr.append((slice_start, slice_end))

    return slices_arr


def update(db_conninfo):
    # TODO find the last update jobs if existent and start from there
    conn = connect(db_conninfo)
    with conn.cursor() as cur:
            cur.execute("""
                SELECT "end" FROM jobs WHERE "group"='update' ORDER BY "end" DESC LIMIT 1
            """)
            if cur.rowcount > 0:
                (last_end, ) = cur.fetchone()
            else:
                last_end = datetime.now().astimezone() - timedelta(days=7)
            
    conn.close()
    
    # Be really careful with the datetimes: datetime.now() returns a naive datetime with no
    # timezone information attached. Postgres always gives us datetimes in local time but with tz attached.
    # .astimezone() attaches the current local timezone information.
    start = last_end.replace(second=0, microsecond=0)
    end = (datetime.now().astimezone() - timedelta(minutes=15)).replace(second=0, microsecond=0)

    sliced = slice(start, end, 60)
    
    job_manager = JobManager(db_conninfo)
    job_manager.create_jobs(
        [
            CreateScrapeJob(
                from_date=start,
                to_date=end,
                query="",
                group="update",
                time_key="created"
            )
            for start, end in sliced
        ]
    )
    job_manager.create_jobs(
        [
            CreateScrapeJob(
                from_date=start,
                to_date=end,
                query="",
                group="update",
                time_key="closed"
            )
            for start, end in sliced
        ]
    )
    job_manager.close()
    
    for start,end in sliced:
        click.echo(f"Submitting: {start} {end}")

    click.echo(f"✅ Submitted {2*len(sliced)} jobs from {start} .. {end}")
    

def backfill(db_conninfo):
    # 2025-05-15 00:00:00 UTC
    start = datetime(2025, 5, 15, 0, 0, 0,0, timezone.utc).astimezone()
    end = datetime.now().astimezone()

    sliced = slice(start, end, 60)

    job_manager = JobManager(db_conninfo)
    job_manager.delete_all("backfill")
    job_manager.create_jobs(
        [
            CreateScrapeJob(from_date=start, to_date=end, query="", group="backfill", time_key="created")
            for start, end in sliced
        ]
    )
    job_manager.close()

    click.echo(f"✅ Submitted {len(sliced)} jobs")


def stats(group, db_conninfo):
    conn = connect(db_conninfo)
    
    with conn.cursor() as cur:
        def total():
            cur.execute('SELECT COUNT(*) FROM jobs WHERE "group" = %s', (group,))
            return cur.fetchone()[0]
        
        def done():
            cur.execute('SELECT COUNT(*) FROM jobs WHERE status=\'done\' AND "group" = %s', (group,))
            return cur.fetchone()[0]
        
        bar = tqdm(total=total(),initial=done(),smoothing=0.0)
        
        while True:
            bar.update(done() - bar.n)   
            time.sleep(.5)
        
    conn.close()


def monitor(db_conninfo):
    print('👀 Monitoring jobs...')
    
    while True:
        conn = connect(db_conninfo)
        with conn.cursor() as cursor:
            cursor.execute("""
                UPDATE jobs
                SET status = 'open', failure_count = failure_count + 1
                WHERE status = 'failed' OR (status = 'running' AND started_at < NOW() - interval '1h' AND failure_count < 10)
                RETURNING *;
            """)
            resetted = cursor.fetchall()
        
        conn.commit()
        conn.close()
        
        if len(resetted) > 0:
            print(f'⚠️ Reset {len(resetted)} jobs that were either stuck or failed.')
            
        time.sleep(60)


