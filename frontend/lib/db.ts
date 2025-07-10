import { Pool, types } from 'pg';
import { AgentType } from './agents';
import { AgentOverview, BinnedInsight, InsightFilter, InsightItem, Release } from './types';

types.setTypeParser(1114, str => new Date(str + 'Z'));

const pool = new Pool({
  connectionString: `${process.env.DATABASE_URL}?application_name=${'frontend'}`,
});

export async function getLastRelease(): Promise<Release> {
  const client = await pool.connect();

  const res = await client.query(`SELECT doi, url, date::text FROM releases ORDER BY date DESC LIMIT 1`);

  client.release();

  return {
    doi: res.rows[0]['doi'],
    url: res.rows[0]['url'],
    date: res.rows[0]['date']
  };
}


export async function getLastUpdated(): Promise<string> {
  const client = await pool.connect();

  const res = await client.query(`SELECT value FROM metadata WHERE key='last_updated'`);

  client.release();

  return res.rows[0]['value'];
}

export async function getBinnedInsight(table: string, x: string, y: string, order: string, bounds: boolean = false): Promise<BinnedInsight> {
  const client = await pool.connect();

  const { rows } = bounds ? await client.query(`
    SELECT
      agent,
      filter,
      ${x} as key,
      ${y} as value,
      ${y}_lb as lower,
      ${y}_ub as upper
    FROM ${table}
    ORDER BY ${order}
    `) : await client.query(`
    SELECT
      agent,
      filter,
      ${x} as key,
      ${y} as value
    FROM ${table}
    ORDER BY ${order}
    `);

  client.release();

  const grouped: Record<InsightFilter, Record<string, InsightItem>> = {};
  const keys: string[] = [];

  for (const row of rows) {
    const { key, agent, filter, value }: { key: string, agent: string, filter: string, value: string } = row;

    if (!(key in keys)) { keys.push(key) }

    if (!(filter in grouped)) {
      grouped[filter] = {};
    }

    if (!(key in grouped[filter])) {
      grouped[filter][key] = {};
    }

    if (!(agent in grouped[filter][key])) {
      if (bounds) {
        const { lower, upper } = row
        grouped[filter][key][agent as AgentType] = { value: parseFloat(value), lower: parseFloat(lower), upper: parseFloat(upper)};
      } else {
        grouped[filter][key][agent as AgentType] = { value: parseFloat(value)};
      }
    }
  }

  const binnedInsight: BinnedInsight = Object.fromEntries(Object.entries(grouped).map(([filter, insight]) => ([
    filter,
    Object.entries(insight).sort(([a,],[b,]) => keys.indexOf(a)-keys.indexOf(b))
  ])))

  return binnedInsight;
}

export async function getOverview(): Promise<Record<AgentType, AgentOverview>> {
  const client = await pool.connect();

  const res = await client.query(`
      SELECT
        agent,
        total_prs,
        merged_prs,
        closed_prs
      FROM insight_overview
      ORDER BY total_prs DESC;
    `);

  client.release();

  return Object.fromEntries(res.rows.map((r: any) => ([
    r['agent'],
    {
      total_prs: r['total_prs'],
      merged_prs: r['merged_prs'],
      closed_prs: r['closed_prs'],
      first_seen: r['first_seen'],
      additions: r['additions'],
      deletions: r['deletions'],
      files_changed: r['changed_files']
    } as AgentOverview
  ])));
}