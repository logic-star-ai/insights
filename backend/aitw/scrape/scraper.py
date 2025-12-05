
from datetime import datetime, timezone
import requests
import logging
import time

from typing import Iterator, Optional

from aitw.database.pull_request import Actor, Comment, Commit, CommitAuthor, PullRequest, PullRequestFile
from aitw.database.repository import Repository

DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

class GitHubScraper:
    batch_size = 25
    
    def __init__(self, token: str, time_key='created'):
        self.url = "https://api.github.com/graphql"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        }
        self.pbar = None
        self.total_expected = None
        self.time_key = time_key
        
    def request_and_backoff(self, query, metadata=None):
        backoff = 1
        while True:
            try:
                logging.info(f'👉 Request: {metadata or ""}')
                
                start_req_time = time.time()
                response = requests.post(
                    self.url, json={"query": query}, headers=self.headers, timeout=60
                )
                if response.status_code == 200:
                    data = response.json()

                    if "errors" in data:
                        logging.info(f"❌ GraphQL Error: {data['errors']}")
                    else:
                        logging.info(f"💰 The last query cost {data['data']['rateLimit']['cost']} points. Remaining {data['data']['rateLimit']['remaining']}")
                        logging.info(f'✅ {response.status_code} in {(time.time() - start_req_time)*1000:.0f}ms')
                        return response
                elif response.status_code >= 500 and response.status_code < 600:
                    logging.info(f"❌ Internal Error {response.status_code} {response.text}")
                    raise TimeoutError()
                else:
                    logging.info(f"❌ Status Code {response.status_code} {response.text}")
                    
                if response is not None:
                    header_reset_in = response.headers.get("X-RateLimit-Reset")
                    header_retry_after = response.headers.get("Retry-After")
                    
                    if header_reset_in is not None:
                        reset_in = int(header_reset_in) - int(time.time())
                        if reset_in > 0:
                            logging.info(f"🔁 Reset rate limit in {reset_in}s, sleeping for that amount!")    
                            time.sleep(reset_in)
                        
                    if header_retry_after is not None:
                        backoff = max(backoff, int(header_retry_after))
                        
            except requests.exceptions.Timeout:
                logging.info("❌ requests.exceptions.Timeout")
            except requests.exceptions.ConnectionError:
                logging.info("❌ requests.exceptions.ConnectionError")
            except requests.exceptions.ChunkedEncodingError:
                logging.info("❌ requests.exceptions.ChunkedEncodingError")
                
            if backoff > 300:
                raise TimeoutError()
                
            logging.info(f'⏳ Retrying in {backoff}s')
            time.sleep(backoff)
            backoff *= 2 
        
    def count(self, start_date: datetime, end_date: datetime, filter: str):
        # From here on we only talk about UTC date strings
        start_date_str: str = start_date.astimezone(timezone.utc).strftime(DATE_FORMAT)
        end_date_str: str = end_date.astimezone(timezone.utc).strftime(DATE_FORMAT)
        
        query = self.build_query(filter=filter, start_date=start_date_str, end_date=end_date_str, first=0, after=None)
        
        response = self.request_and_backoff(query)
        data = response.json()
        return data["data"]["search"]["issueCount"]

    def scrape(self, start_date: datetime, end_date: datetime, filter: str) -> Iterator[PullRequest | Repository | None]:
        # From here on we only talk about UTC date strings
        start_date_str = start_date.astimezone(timezone.utc).strftime(DATE_FORMAT)
        end_date_str = end_date.astimezone(timezone.utc).strftime(DATE_FORMAT)
        
        curr_start_date = start_date_str # Current start to bypass 1,000 search limit
        curr_after = None # Cursor based pagination
        num_res_page = 0 # Number of results returned on current page (must be < 1,000)
        curr_batch_size = self.batch_size

        while True:
            query = self.build_query(
                filter=filter,
                start_date=curr_start_date,
                end_date=end_date_str,
                first=curr_batch_size,
                after=curr_after
            )
            try:
                response = self.request_and_backoff(query, {'batch_size': curr_batch_size, 'start_date':curr_start_date, 'after': curr_after})
                
                data = response.json()

                search_data = data["data"]["search"]
                items = search_data["nodes"]
                print(f'Retuned {len(items)} items')

                for item in items:
                    yield PullRequest(
                        id=item['fullDatabaseId'],
                        url=item['url'],
                        title=item['title'],
                        body=item['bodyText'],
                        agent = None,
                        actor=Actor(
                            login=item['author'] and item['author']['login'],
                            type=item['author'] and item['author']['__typename']
                        ),
                        created_at = datetime.strptime(item['createdAt'], DATE_FORMAT).replace(tzinfo=timezone.utc).astimezone(),
                        closed_at = datetime.strptime(item['closedAt'], DATE_FORMAT).replace(tzinfo=timezone.utc).astimezone() if item['closedAt'] else None,
                        isMerged = item['mergedAt'] is not None,
                        isDraft= item['isDraft'],
                        additions = item['additions'],
                        deletions = item['deletions'],
                        changed_files = item['changedFiles'],
                        commits = item['commits']['totalCount'],
                        comments = item['comments']['totalCount'],
                        reviews = item['reviews']['totalCount'],
                        
                        base_ref = item['baseRefName'],
                        head_ref = item['headRefName'],
                        base_repo_id = item['baseRepository'] and item['baseRepository']['databaseId'],
                        head_repo_id = item['headRepository'] and item['headRepository']['databaseId'],
                        commitsList = [
                            Commit(authors=[
                                CommitAuthor(name=x['name'], email=x['email'])
                                for x in n['commit']['authors']['nodes']
                            ]) 
                            for n in item['commits']['nodes'] if n is not None
                        ],
                        files = [
                            PullRequestFile(additions=n['additions'], deletions=n['deletions'], path=n['path']) 
                            for n in item['files']['nodes'] if n is not None
                        ] if item['files'] is not None else None,
                        commentsList = [
                            Comment(
                                id=n['databaseId'], 
                                created_at=n['createdAt'],
                                author=Actor(login=n['author']['login'], type=n['author']['__typename']) if n['author'] is not None else None,
                                body=n['bodyText']) 
                            for n in item['comments']['nodes'] if n is not None
                        ] if item['comments'] is not None else None, 
                    )
                    
                    if item['baseRepository']:
                        yield Repository(
                            id=item['baseRepository']['databaseId'],
                            name=item['baseRepository']['nameWithOwner'],
                            url=item['baseRepository']['url'],
                            is_fork=item['baseRepository']['isFork'],
                            stargazers=item['baseRepository']['stargazerCount'],
                            watchers=item['baseRepository']['watchers']['totalCount'],
                            forks=item['baseRepository']['forkCount'],
                            primary_language=item['baseRepository']['primaryLanguage'] and item['baseRepository']['primaryLanguage']['name']
                        )
                        
                    if item['headRepository']:
                        yield Repository(
                            id=item['headRepository']['databaseId'],
                            name=item['headRepository']['nameWithOwner'],
                            url=item['headRepository']['url'],
                            is_fork=item['headRepository']['isFork'],
                            stargazers=item['headRepository']['stargazerCount'],
                            watchers=item['headRepository']['watchers']['totalCount'],
                            forks=item['headRepository']['forkCount'],
                            primary_language=item['headRepository']['primaryLanguage'] and item['headRepository']['primaryLanguage']['name']
                        )

                if not items:
                    return

                if num_res_page + 2*curr_batch_size < 1000:
                    curr_after = search_data["pageInfo"]["endCursor"]
                    num_res_page += curr_batch_size
                elif curr_start_date == items[-1]["createdAt"]:
                    logging.error('‼ We have a problem. We maxed out the GraphQL page limit but the start date did not change :(')
                    raise ValueError('Maxed out the GraphQL page limit but start date did not change :(')
                else:
                    curr_start_date = items[-1]["createdAt"]
                    curr_after = None
                    
                curr_batch_size = self.batch_size
                

                if search_data["issueCount"] <= self.batch_size:
                    return

            except KeyboardInterrupt:
                logging.error("\n❌ Interrupted by user. Exiting.")
                exit(1)
            
            except TimeoutError:
                logging.warning(f"⚠️ Timeout: Reducing batch_size and retry! (batch_size {curr_batch_size} -> {curr_batch_size//2})")
                if curr_batch_size > 1:
                    curr_batch_size //= 2
            except Exception as e:
                logging.error(f"⚠️ Exception occurred: {e}")
                logging.error(e)
                raise e
            finally:
                # Gives the caller time to react in long retry loops cycles
                yield None

    def build_query(self, filter: str, start_date: str, end_date: str, first: int, after: Optional[str]) -> str:
        return f"""
        query {{
            rateLimit {{
                cost
                remaining
            }}
            search(type: ISSUE, query: "is:pr {filter} {self.time_key}:{start_date}..{end_date} sort:created-asc", first: {first}, after: {f'"{after}"' if after is not None else 'null'}) {{
                issueCount
                pageInfo {{
                    endCursor
                    hasNextPage
                }}
                nodes {{
                    ... on PullRequest {{
                        fullDatabaseId
                        title
                        url
                        
                        bodyText
                        
                        createdAt
                        mergedAt
                        closedAt
                        updatedAt
                        isDraft
                        
                        changedFiles
                        additions
                        deletions
                        
                        author {{ login, __typename }}
                        
                        comments(first:100) {{
                            totalCount
                            nodes {{
                                databaseId
                                createdAt
                                author {{
                                    login
                                    __typename
                                }}
                                bodyText  
                            }}
                        }}
                        
                        reviews {{
                            totalCount
                        }}
                        
                        commits(first: 1) {{
                            totalCount
                            nodes {{
                                commit {{
                                    authors(first:2) {{
                                        nodes {{
                                            name
                                            email
                                        }}
                                    }}
                                }}
                            }}
                        }}
                        
                        files(first:100) {{
                            nodes {{
                                additions
                                deletions
                                path
                            }}
                        }}
                        
                        baseRefName
                        baseRepository {{
                            nameWithOwner
                            stargazerCount
                            databaseId
                            url
                            isFork
                            forkCount
                            watchers {{
                                totalCount
                            }}
                            primaryLanguage {{
                              name
                            }}
                        }}
                        
                        headRefName
                        headRepository {{
                            nameWithOwner
                            stargazerCount
                            databaseId
                            url
                            isFork
                            forkCount
                            watchers {{
                                totalCount
                            }}
                            primaryLanguage {{
                              name
                            }}
                        }}
                    }}
                }}
            }}
        }}
        """
    
    