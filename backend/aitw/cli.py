import os
import uuid
import click

import aitw.insights.insights as insights_file
import aitw.scrape.worker as scrape_worker
import aitw.scrape.manager as scrape_manager
import aitw.archive.archive as archive_file
import aitw.scrape.pr_classifier as pr_classifier

import dotenv
dotenv.load_dotenv(override=True)

@click.group()
def cli():
    pass

@cli.group()
def scrape():
    pass

@scrape.command()
@click.option('--token', default=lambda: os.getenv('GITHUB_TOKEN'), required=True)
@click.option('--id', default=lambda: uuid.uuid4())
@click.option('--group')
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
def worker(token, id, group, db):
    scrape_worker.worker(token, id, group, db)
    
@scrape.group()
def manager():
    pass

@manager.command()
@click.argument("group")
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
def stats(group, db):
    scrape_manager.stats(group, db)
    
@manager.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
def monitor(db):
    scrape_manager.monitor(db)
    
@manager.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
def update(db):
    scrape_manager.update(db)
    
@manager.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
def backfill(db):
    scrape_manager.backfill(db)
    
@cli.command()
@click.argument('insight', required=True)
@click.option('--db-backend', envvar='POSTGRES_CONNECT_BACKEND', required=True)
@click.option('--db-frontend', envvar='POSTGRES_CONNECT_FRONTEND', required=True)
def insights(insight, db_backend, db_frontend):
    insights_file.insights(insight, db_backend, db_frontend)
    
@cli.group()
def archive():
    pass
    
@archive.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
@click.option('--output', '-o', default='./archive/prs.csv.gz', type=click.Path())
def prs(db, output):
    archive_file.prs(db, output)
    
@archive.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
@click.option('--output', '-o', default='./archive/repos.csv.gz', type=click.Path())
def repos(db, output):
    archive_file.repos(db, output)
    
@archive.command()
@click.option('--output', '-o', default='./archive/website.tar.gz', type=click.Path())
def website(output):
    archive_file.website(output)

@archive.command()
@click.option('--db', envvar='POSTGRES_CONNECT_FRONTEND', required=True)
@click.option('--token', envvar='ZENODO_TOKEN', required=True)
@click.option('--files', multiple=True, 
              default=['./archive/prs.csv.gz', './archive/repos.csv.gz', './archive/website.tar.gz'],
              type=click.Path(exists=True, file_okay=True,))
def upload(db, token, files):
    archive_file.upload(db, token, files)
    
@cli.command()
@click.option('--db', envvar='POSTGRES_CONNECT_BACKEND', required=True)
def reclassify(db):
    pr_classifier.reclassify(db)

if __name__ == '__main__':
    cli()