<div align="center">
  <h1>
    <img height="150px" src="./docs/aitw-banner.png" alt="Agents in the Wild"><br>
    Agents in the Wild
  </h1>

  <a href="https://opensource.org/licenses/MIT">
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT">
  </a>
  <a href="https://github.com/logic-star-ai/insights/actions/workflows/backend.yml">
    <img src="https://github.com/logic-star-ai/insights/actions/workflows/backend.yml/badge.svg" alt="Backend CI">
  </a>
  <a href="https://github.com/logic-star-ai/insights/actions/workflows/frontend.yml">
    <img src="https://github.com/logic-star-ai/insights/actions/workflows/frontend.yml/badge.svg" alt="Frontend CI">
  </a>
</div>

## 👋 Overview

This repository contains the source code for [insights.logicstar.ai](https://insights.logicstar.ai/).  
We track all GitHub pull requests created since **2025-05-15** and analyze them to identify autonomous code agents.

Pull requests are classified using the following rules:

- **Human**: All PRs that do not match any known agent pattern.
- **OpenAI Codex**: PRs where the head branch starts with `codex/`.
- **Google Jules**: PRs where the first commit is authored by `google-labs-jules[bot]`.
- **GitHub Copilot**: PRs where the head branch starts with `copilot/`.
- **Devin**: PRs authored by `devin-ai-integration[bot]`.
- **Cursor Agent**: PRs where the head branch starts with `cursor/`.
- **Claude Code**: PRs where the first commit is authored by `claude` or the head branch starts with `claude/`.
- **OpenHands**: PRs where the first commit is authored by `openhands`.
- **Codegen**: PRs authored by `codegen-sh`.
- **Tembo**: PRs authored by `tembo-io`.
- **Cosine**: PRs where the head branch starts with `cosine/`

## ⚙️ System Architecture

The system consists of:

- A **Next.js frontend** for querying the database and visualizing insights.
- A **Python backend** for scraping GitHub pull requests and generating insights.

<picture>
  <img src="./docs/system.svg" alt="AITW System Overview" style="width:100%">
</picture>

## 🚀 Installation

While we host a live version at [insights.logicstar.ai](https://insights.logicstar.ai/), you can also deploy the system locally.

### 💿 Database

Ensure you have access to a PostgreSQL database.

### 💻 Frontend

Make sure you have `npm` and `node` installed. Then run:

```bash
npm install
npm run build
```

Configure the environment via `frontend/.env.local`:

```
DATABASE_URL=postgres://<user>:<password>@localhost:5433/<db>
```

To start the frontend:

```bash
npm run start
```

### ⚙️ Backend

Install the backend package in development mode:

```bash
pip3 install -e backend
```

You should now have access to the `aitw` CLI tool.

Set the following environment variables:

```bash
POSTGRES_CONNECT="dbname=<dbname> user=<user> password=<passwd> host=<host> port=<port>"
GOOGLE_APPLICATION_CREDENTIALS="<path_to_gcp_app_creds>"
```

We use GCP for logging scraping worker activity. You can omit the credentials if you're not using GCP logging.

To start a scraping worker, ensure the environment is configured and run:

```bash
aitw scrape worker --token <GH_TOKEN>
```

To backfill all PRs since 2025-05-15:

```bash
aitw scrape manager backfill
```

To derive insights from the current database:

```bash
aitw insights hourly
aitw insights daily 
```

To update with new PRs created or closed since the last update:

```bash
aitw scrape manager update
```

We recommend running both the `update` and `insights` commands regularly (e.g., hourly).

## 📝 Changelog
**2025-07-10**: Added “Cite Us” section, released dataset, and added OpenHands, Cosine, Claude Code, and Codegen agents.
**2025-07-08**: Initial release of the dashboard.