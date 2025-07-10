from datetime import date
import os
import gzip
from pathlib import Path
import shutil
import subprocess
import tempfile
import requests

from tqdm import tqdm

from aitw.database.connection import connect

def prs(dbconn, output):
    conn = connect(dbconn)
    
    with conn.cursor() as cur, gzip.open(output, 'wb') as gz:
        copy_sql = "COPY (SELECT * FROM prs WHERE created_at > '2025-05-15') TO STDOUT WITH CSV HEADER"
        with cur.copy(copy_sql) as cop, tqdm(
                unit='B',
                unit_scale=True,
                desc='Exporting prs.csv'
            ) as pbar:
            for data in cop:
                bytes_written = gz.write(data.tobytes())
                pbar.update(bytes_written)
                
    conn.close()
    print(f"✅ Archived prs: {output}")
    
def repos(dbconn, output):
    conn = connect(dbconn)
    
    with conn.cursor() as cur, gzip.open(output, 'wb') as gz:
        copy_sql = "COPY (SELECT * FROM repos) TO STDOUT WITH CSV HEADER"
        with cur.copy(copy_sql) as cop, tqdm(
                unit='B',
                unit_scale=True,
                desc='Exporting repos.csv'
            ) as pbar:
            for data in cop:
                bytes_written = gz.write(data.tobytes())
                pbar.update(bytes_written)
    
    conn.close()
    print(f"✅ Archived repos: {output}")
    
def website(output):
    with tempfile.TemporaryDirectory() as temp_dir:
        site_dir = os.path.join(temp_dir, "site")

        subprocess.run([
            "wget",
            "--mirror",               # recursive download
            "--convert-links",        # fix local links
            "--adjust-extension",     # proper file extensions
            "--page-requisites",      # download CSS/JS/images
            "--no-parent",            # don’t ascend the URL path
            "--directory-prefix", site_dir,
            'https://insights.logicstar.ai'
        ], check=True)

        website_root = Path(site_dir).resolve()
        shutil.make_archive(
            base_name=Path(output).with_suffix('').with_suffix('').as_posix(), 
            format="gztar",
            root_dir=website_root
        )

    print(f"✅ Archived website: {output}")

def upload(dbconn_info, token, files):
    headers = {"Content-Type": "application/json"}
    params = {"access_token": token}

    # Step 1: Create a new version (empty draft)
    r = requests.post("https://zenodo.org/api/deposit/depositions/15846866/actions/newversion", params=params, json={}, headers=headers)
    r.raise_for_status()
    
    deposit = r.json()
    deposit_bucket = deposit["links"]["bucket"]
    deposit_id = deposit['id']
    print(f"[INFO] Created Zenodo deposit ID: {deposit_id}")

    # Step 2: Upload all files)
    for file_path in files:
        filename = os.path.basename(file_path)
        print(f"[INFO] Uploading {filename}...")
        
        with open(file_path, 'rb') as fp:    
            r = requests.put(
                f"{deposit_bucket}/{filename}",
                data=fp,
                params=params,
            )
            r.raise_for_status()

    # Step 3: Add metadata
    version = date.today().strftime('%Y-%m-%d')
    r = requests.put(
        f"https://zenodo.org/api/deposit/depositions/{deposit_id}",
        params=params,
        json={
            "metadata": {
                'title': f'Agents in the Wild - {version}',
                'description': f'''Snapshot of the Agents in the Wild project, including a dataset dump and a snapshot of the live dashboard.
                
                Date of snapshot: {version}''',
                'upload_type': 'dataset',
                'version': version,
                'keywords': [],
                'creators': [
                    {
                    "name": "Christian Mürtz",
                    "type": "DataCurator",
                    "affiliation": "ETH Zurich"
                    },
                    {
                    "name": "Mark Niklas Müller",
                    "type": "ProjectLeader",
                    "affiliation": "ETH Zurich"
                    }
                ],
                "related_identifiers": [
                    {
                    "identifier": "https://github.com/logic-star-ai/insights",
                    "relation": "isSupplementTo",
                    "scheme": "url",
                    "resource_type": "software"
                    },
                    {
                    "identifier": "https://insights.logicstar.ai/",
                    "relation": "isReferencedBy",
                    "scheme": "url",
                    "resource_type": "other"
                    }
                ]
            }
        },
        headers=headers
    )
    r.raise_for_status()
    print("✅ Metadata successfully attached.")

    # # Step 4: Publish
    r = requests.post(
        f"https://zenodo.org/api/deposit/depositions/{deposit_id}/actions/publish",
        params=params
    )
    r.raise_for_status()

    doi = r.json()['doi']
    url = r.json()['record_url']
    
    print(f"✅ Published successfully! DOI: {doi}")
    
    # Step 5: Add to releases table in database
    conn = connect(dbconn_info)    
    with conn.cursor() as cur:
        cur.execute("INSERT releases (doi, date, url) VALUES (%s, %s, %s)", (doi, version, url))
    conn.close()
