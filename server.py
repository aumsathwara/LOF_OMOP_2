import os
import multiprocessing
import rpy2.robjects as robjects
from mcp.server.fastmcp import FastMCP, Context
from etl_script import OmopPipeline
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
import time

typename = "OMOPSync"
mcp = FastMCP(name=typename)
pipeline = OmopPipeline()

# === CONFIGURATION ===
POSTGRES_CONFIG = {
    "dbname": "omop",
    "user": "postgres",
    "password": "user",
    "host": "localhost",
    "port": "5432"
}

SQL_FILE_PATH = "results_schema.sql"
SQL_FILE_PATH2 = "concept_counts.sql"

# Mac/Linux: Path Example: /Users/cpu/Downloads/OMOP/
# Windows: Path Example: D:\\LoF\\omop\\
OMOP_PATH = "."

# path to OMOP/csv folder E.g. /Users/cpu/Downloads/OMOP/csv
CSV_PATH = "data/csv"
VOCAB_CSV = "vocabulary_csv"

# Mac/Linux: outputFolder Example: /Users/cpu/Downloads/OMOP/output
# Windows: outputFolder Example: D:\\LoF\\omop\\output
OUTPUT_PATH = "output"
ANALYSIS = "analysis"

db_url = (
    f"postgresql://"
    f"{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@"
    f"{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/"
    f"{POSTGRES_CONFIG['dbname']}"
)

engine = create_engine(db_url)

# ─── TOOL 1: Query DB SQL → JSON ─────────────────────────────────────────────────
@ mcp.tool(
    name="query_database",
    description="Run an arbitrary SELECT SQL and return the rows as JSON."
)
def query_database(sql: str) -> list[dict]:
    try:
        df = pd.read_sql(sql, engine)
        records = df.to_dict(orient="records")
        return [
            {
                "type": "json",
                "text": records
            }
        ]
    except Exception as e:
        # Return the error in JSON form
        return [
            {
                "type": "json",
                "text": { "error": str(e) }
            }
        ]

# ─── TOOL 2: Plot Analysis graphs SQL → PLOT (base64 PNG) ───────────────────────────────────────────────
@ mcp.tool(
    name="plot_query",
    description=(
        "Run a SQL query and save a chart. "
        "Args: sql, chart_type ('line','bar','heatmap'), x_field, y_field."
    )
)
def plot_query(
    sql: str,
    chart_type: str = "line",
    x_field: str = None,
    y_field: str = None
) -> list[dict]:
    try:
        # 1. Run query
        df = pd.read_sql(sql, engine)

        # 2. Build plot
        plt.close("all")
        fig, ax = plt.subplots(figsize=(8, 5))
        if chart_type == "line":
            df.plot(x=x_field, y=y_field, ax=ax)
        elif chart_type == "bar":
            df.plot.bar(x=x_field, y=y_field, ax=ax)
        elif chart_type == "heatmap":
            pivot = df.pivot(index=y_field, columns=x_field, values="value")
            sns.heatmap(pivot, annot=True, fmt=".0f", ax=ax)
        elif chart_type == "scatter":
            ax.scatter(df[x_field], df[y_field])
            ax.set_xlabel(x_field)
            ax.set_ylabel(y_field) 
        else:
            return [{"type": "text", "text": f"Unknown chart_type: {chart_type}"}]

        ax.set_title(f"{chart_type.title()} plot")

        # 3. Ensure output directory exists
        os.makedirs(ANALYSIS, exist_ok=True)

        # 4. Save to file with timestamp
        timestamp = int(time.time())
        filename = f"plot_{chart_type}_{timestamp}.png"
        filepath = os.path.join(ANALYSIS, filename)
        fig.tight_layout()
        fig.savefig(filepath, format="png")

        return [
            {
                "type": "text",
                "text": f"Plot saved to '{filepath}'."
            }
        ]
    except Exception as e:
        return [
            {
                "type": "text",
                "text": f"Error generating plot: {str(e)}"
            }
        ]
            
# ─── TOOL 3: Ensures/Create Schemas ───────────────────────────────────────────────
@ mcp.tool(
    name="ensure_schemas", 
    description="Ensure required OMOP schemas exist"
)
def ensure_schemas() -> list:
    try:
        pipeline.ensure_schemas_exist()
        return [{"type": "text", "text": "Schemas ensured successfully."}]
    except Exception as e:
        return [{"type": "text", "text": f"Error ensuring schemas: {str(e)}"}]

# ─── TOOL 4: Runs ETL Pipeline (Embeded VOCAB and CSV into OMOP CDM) ───────────────────────────────────────────────
@ mcp.tool(
    name="run_etl", 
    description="Run ETL process to build CDM and load Synthea data"
)
def run_etl() -> list:
    try:
        # Start the background task for ETL pipline
        process = multiprocessing.Process(target = pipeline.run_etl_process)
        process.start()  
        return [{"type": "text", "text": "ETL process started in the background..."}]
    except Exception as e:
        return [{"type": "text", "text": f"Error running ETL process: {str(e)}"}]

# ─── TOOL 5: Runs SQL query against DB ───────────────────────────────────────────────
@ mcp.tool(
    name="run_sql", 
    description="Execute a SQL file against the OMOP database"
)
def run_sql_file(filepath: str) -> list:
    try:
        # Start the background task for SQL queries
        process = multiprocessing.Process(target= pipeline.run_sql_file(filepath))
        process.start()
        return [{"type": "text", "text": f"SQL file '{filepath}' executed successfully."}]
    except Exception as e:
        return [{"type": "text", "text": f"Error executing SQL file '{filepath}': {str(e)}"}]


# ─── TOOL 6: Runs Achilles ───────────────────────────────────────────────
@ mcp.tool(
    name="run_achilles", 
    description="Run Achilles analysis on OMOP CDM data"
)
def run_achilles() -> list:
    try:
        # Start the background task for Achilles
        process = multiprocessing.Process(target= pipeline.run_achilles)
        process.start()
        return [{"type": "text", "text": "Achilles analysis started in background."}]
    except Exception as e:
        return [{"type": "text", "text": f"Error running Achilles: {str(e)}"}]

# ─── TOOL 7: Runs Data Quality Checks and the Dashboard ───────────────────────────────────────────────
@mcp.tool(name='run_dqd_checks', description='Execute Data Quality Dashboard checks')
def run_dqd_checks():
    try:
        # Start the background task for DQD checks
        process = multiprocessing.Process(target= pipeline.run_dqd_checks)
        process.start()     
        return [{'type':'text','text':'DQD checks started in the background.'}]
    except Exception as e:
        return [{'type':'text','text':f'Error running DQD checks: {e}'}]

# ─── TOOL 8: Runs ETL + Achilles + DQD all together ───────────────────────────────────────────────
@ mcp.tool(
    name="run_all", 
    description="Run full OMOP ETL pipeline end-to-end"
)
def run_all() -> list:
    try:
        # Start the background task for all together
        process = multiprocessing.Process(target= pipeline.run_all)
        process.start() 
        return [{"type": "text", "text": "Full pipeline execution started in background."}]
    except Exception as e:
        return [{"type": "text", "text": f"Error running full pipeline: {str(e)}"}]

if __name__ == "__main__":
    mcp.run(transport='stdio')
