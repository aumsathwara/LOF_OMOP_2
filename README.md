# OMOPSync Readme

> **One command to spin up an OMOP Common Data Model (CDM) instance, run quality checks, and query it with an LLM-powered agent.**  

This repository contains three tightly‑coupled pieces:

| Piece | Purpose |
|-------|---------|
| **`server.py`** | Exposes the ETL/analytics pipeline as Model‑Context‑Protocol (MCP) tools so a language model can call them.  |
| **`etl_script.py`** | All R and SQL orchestration needed to transform Synthea CSVs → OMOP 5.4, run **Achilles** + **Data Quality Dashboard**, and create helper indices.|
| **`client.py`** | A thin command‑line interface that connects a Gemini model to the MCP server, forwards user questions, and returns answers + token usage.|

`requirements.txt` pins every Python dependency (≈110 packages) so the whole stack is reproducible. 

---

## 1  Architecture at a Glance

```
┌────────┐  user prompt   ┌───────────────┐ stdio JSON/RPC ┌──────────┐
│ CLI    │──────────────▶│ Gemini 1.5 LLM │───────────────▶│ MCP      │
│ (async)│◀──────────────│ (tools schema) │◀───────────────│ server.py│
└────────┘   natural‑lang └───────────────┘   tool calls   └──────────┘
                                                           │  ETL &  │
                                                           │ plotting│
                                                           └──────────┘
```

1. **User types a question** (e.g. *“Plot admissions by age bucket.”*).  
2. `client.py` streams available tool signatures to Gemini, then sends the prompt.  
3. Gemini decides which MCP tool to call (`plot_query`, `run_etl`, …).  
4. `server.py` executes Python, SQL, or R in a **background process** where needed, persists artefacts under `analysis/`, and returns paths or JSON.  
5. The CLI prints the answer plus exact token counts.

---

## 2  Prerequisites

| Requirement | Tested Version(s) | Notes |
|-------------|-------------------|-------|
| **Python**  | 3.9 – 3.12        | `rpy2` must match your local R build. |
| **R**       | 4.3 or newer      | Set `R_HOME` or let the script fall back to `C:\Program Files\R\R-4.3.0`. |
| **PostgreSQL** | 15+            | Database `omop` and super‑user `postgres`/`user` expected, but credentials are configurable. |
| **Synthea data** | any CSV dump | Place under `data/csv/` or change `CSV_PATH`. |
| **Google Gemini API key** | – | Set env var `GEMINI_API_KEY`. |
| **Build tools** | gcc/clang & Rtools (Windows) | Needed to compile R packages such as `Rcpp` during the first run. |

> **Windows users:** run everything from an *“x64 Native Tools Command Prompt for VS”* or WSL to ensure C toolchain availability.

---

## 3  Quick‑Start (15‑line demo)

```bash
# 1. Clone repo & enter
git clone https://github.com/your-org/omopsync.git
cd omopsync

# 2. Python env
python -m venv .venv
. .venv/bin/activate           # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 3. Environment
export GEMINI_API_KEY="sk-…"   # or set in a .env file
export R_HOME="/usr/lib/R"     # customize for your OS
# (optional) tweak Postgres creds here

# 4. From shell, start the chat client
python client.py server.py
```

Then try a session:

```
Query: Ensure all schemas in database
Query: Run etl process for me
Query: Get me list of all person from cdm54 schema and person table
Query: Plot admissions by sex bar chart
Query: quit
```

---

## 4  Detailed Setup

### 4.1  Python dependencies
`pip install -r requirements.txt` will fetch:

* **MCP 1.6** – JSON‑RPC transport and tool decorators  
* **google‑genai** – Gemini SDK  
* **rpy2 3.5** – Transparent bridge to R  
* Data stack: **pandas**, **SQLAlchemy**, **matplotlib**, **seaborn**  

A few packages (e.g. `grpcio` ≥ 1.71) require a C compiler.

### 4.2  R environment

`etl_script.py` installs **ETL‑Synthea**, **Achilles**, and **DataQualityDashboard** directly from GitHub on the first run. All R output is silenced with `options(warn=-1)` and redirected via `contextlib.redirect_stdout`. 

> The first ETL invocation can take 10‑15 min while binaries compile.

### 4.3  Database

The pipeline expects three schemas:

* `synthea` – raw CSV ingest  
* `cdm54` – fully mapped OMOP v5.4 tables  
* `results` – Achilles & DQD outputs  

`ensure_schemas_exist()` runs automatically before ETL to create them if missing. 

### 4.4 Using ATLAS

Prerequisite: This guide relies on you already having installed ATLAS and WebAPI and have it running. See the guide below for information on how to do this.

Since the etl script populates the Postgres database with new schemas, now we want to get those schemas and populate the ATLAS dashboard with them. Basically we need to set up the ATLAS dashboard with those schemas as "Data sources". There are multiple ways to do this(editing a datasources.json or running SQL scripts), but in essence what we want to do is to tell ATLAS what schemas to use for specific information. So it's important that the ETL script has been run at this point and created them in the DB.

For me, I mapped the following:
"cdmSchema": "cdm54",
"resultsSchema": "results",
"vocabSchema": "cdm54",
"tempSchema": "tmp" # This one I created as an empty schema after the ETL script was run

But as the ATLAS setup is still a work in progress, it is important that you check your own schema and ensure that you map the correct one.

Please see this link for more information on one way to do this: https://github.com/OHDSI/WebAPI/wiki/CDM-Configuration

### 4.5 Installing ATLAS and WebAPI

Important notes: The webapi verison you use should be the same as the java version you use(WebAPI 8.5 and java 8 in our case)


Useful links: 
Atlas Setup - https://github.com/OHDSI/Atlas/wiki/Atlas-Setup-Guide
WebAPI - https://github.com/OHDSI/WebAPI - Get the same version as the java you use. As you see later, 
we have used java 8. So a direct link to that is https://apache.root.lu/tomcat/tomcat-8/v8.5.93/bin/
WebAPI Installation - https://github.com/OHDSI/WebAPI/wiki/WebAPI-Installation-Guide
PostgreSQL Setup - https://github.com/OHDSI/WebAPI/wiki/PostgreSQL-Installation-GuideWebAPI Java 8 - if you're using a Mac with an M series chip, you'll need to use Azul Zulu which can be downloaded here:
https://www.azul.com/downloads/?version=java-8-lts&os=macos&architecture=arm-64-bit&package=jdk#zulu

---

## 5  Tool Catalogue

| Tool | Signature | Typical use |
|------|-----------|-------------|
| `ensure_schemas` | `()` | Idempotent check/creation of `cdm54`, `synthea`, `results`. |
| `run_etl` | `()` | Loads Synthea CSVs → OMOP, builds indices, background process. |
| `run_achilles` | `()` | Population‑level characterization. |
| `run_dqd_checks` | `()` | Executes >3 000 CDM data‑quality rules. |
| `run_sql_file` | `(filepath)` | Apply helper DDL such as `results_schema.sql`. |
| `query_database` | `(sql)` | Free‑form `SELECT`; returns JSON list. |
| `plot_query` | `(sql, chart_type, x_field, y_field)` | Saves `analysis/plot_*_TIMESTAMP.png`, returns path. |

All tools return **lists of blocks** so the LLM can show either plain text or JSON.

---

## 6  Client Walk‑through

1. **Connect** – The client starts the server as a sub‑process with the same Python interpreter and captures its stdin/stdout pipes. 
2. **Discover** – `list_tools()` exposes every `@mcp.tool`.  
3. **Schema packing** – It rewrites each tool’s JSON Schema into Gemini’s [`function_declarations`].  
4. **Prompt** – Tool schema + user text → Gemini 1.5 Pro.  
5. **Streaming function calls** – For each `function_call` part, the client invokes the server and prints returned blocks.  
6. **Token metering** – `response.usage_metadata` is surfaced at the end of every answer.

The loop continues until the user types `quit`.

---

## 7  Directory Layout

```
.
├── client.py            # gemini‑powered CLI
├── server.py            # MCP + ETL tools
├── etl_script.py        # heavy‑lifting ETL/analytics
├── requirements.txt
├── data/
│   └── csv/             # place synthea csvs here
├── vocabulary_csv/      # OHDSI vocabularies
├── output/              # DQD JSON etc.
└── analysis/            # charts saved by plot_query
```

`analysis/` and `output/` are created on demand.

---


## 8  Troubleshooting & FAQ

| Symptom | Likely cause / fix |
|---------|--------------------|
| `RuntimeError: GEMINI_API_KEY environment variable is required` | Export the key or create a `.env` file. |
| `EnvironmentError: R_HOME not set` | `export R_HOME` (Linux/Mac) or set *System Variables → R_HOME* (Windows). |
| `psycopg2.OperationalError: could not connect to server` | Verify Postgres is running on `localhost:5432` and credentials match. |
| R compile errors (Windows) | Install **Rtools 4.3** and add it to `%PATH%`. |
| Plot saved but path not printed | Provide both `x_field` **and** `y_field`. E.g. `chart_type='bar', x_field='gender_concept_id', y_field='count'`. |

---

## 9  Extending the Project

* **Swap the LLM** – Replace Gemini with any JSON‑function‑calling model; only `client.py` changes.  
* **Add a new analysis** – Decorate a Python function with `@mcp.tool`; it will be auto‑discovered.  
* **Containerization** – Build three stages: base (Python+R), database, UI. Mount `analysis/` as a volume to share plots.

---

## 10  License

MIT — see `LICENSE`.

---

## 11  Citation

If you use OMOPSync in academic work, please cite:

```bibtex
@software{omopsync_2025,
  title        = {OMOPSync},
  author       = {Merlin Simoes, Aum Sathwara, Bjoern Sagstad, Harneet Dehiya, Vishnu Shanmugavel},
  year         = 2025,
  url          = {https://github.com/aumsathwara/omopsync}
}
```