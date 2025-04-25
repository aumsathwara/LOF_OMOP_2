import rpy2.robjects as robjects
import psycopg2
import os

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

# Functions to run R and SQL scripts

def run_r_script(script_str):
    print("Running R script...")
    robjects.r(script_str)
    print("R script finished.\n")

def run_sql_file(filepath):
    print(f"Running SQL file: {filepath}")
    try:
        with open(filepath, 'r') as f:
            sql_script = f.read()

        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()

        statements = sql_script.strip().split(';')
        for stmt in statements:
            stmt = stmt.strip()
            if stmt:
                cursor.execute(stmt + ';')

        conn.commit()
        cursor.close()
        conn.close()
        print("SQL script executed successfully.\n")

    except Exception as e:
        print(f"Error executing SQL file {filepath}:", e)


def run_etl_process():
    etl_r_script = f"""
    devtools::install_github("OHDSI/ETL-Synthea")
    library(ETLSyntheaBuilder)

    cd <- DatabaseConnector::createConnectionDetails(
      dbms         = "postgresql",
      server       = "localhost/omop",
      user         = "postgres",
      password     = "user",
      port         = 5432,
      pathToDriver = "{OMOP_PATH}"
    )

    cdmSchema      <- "cdm54"
    cdmVersion     <- "5.4"
    syntheaVersion <- "3.3.0"
    syntheaSchema  <- "synthea"
    vocabFileLoc   <- "{VOCAB_CSV}"
    syntheaFileLoc <- "{CSV_PATH}"
    resultsSchema  <- "results"
    cdmSourceName  <- "synthea"

    # Create CDM tables, but ignore “already exists” errors
    tryCatch(
      ETLSyntheaBuilder::CreateCDMTables(
        connectionDetails = cd,
        cdmSchema         = cdmSchema,
        cdmVersion        = cdmVersion
      ),
      error = function(e) {{
        if (grepl("already exists", e$message, ignore.case=TRUE)) {{
          message("CDM tables already exist; skipping CreateCDMTables")
        }} else {{
          stop(e)
        }}
      }}
    )

    # Load vocabulary CSVs, but skip if already loaded
    tryCatch(
      ETLSyntheaBuilder::LoadVocabFromCsv(
        connectionDetails = cd,
        cdmSchema         = cdmSchema,
        vocabFileLoc      = vocabFileLoc
      ),
      error = function(e) {{
        if (grepl("already exists", e$message, ignore.case=TRUE)) {{
          message("Vocabulary already populated; skipping LoadVocabFromCsv")
        }} else {{
          stop(e)
        }}
      }}
    )

    # Source-data tables
    ETLSyntheaBuilder::CreateSyntheaTables(
      connectionDetails = cd,
      syntheaSchema     = syntheaSchema,
      syntheaVersion    = syntheaVersion
    )
    ETLSyntheaBuilder::LoadSyntheaTables(
      connectionDetails = cd,
      syntheaSchema     = syntheaSchema,
      syntheaFileLoc    = syntheaFileLoc
    )

    # Map & rollup tables
    tryCatch(
      ETLSyntheaBuilder::CreateMapAndRollupTables(
        connectionDetails = cd,
        cdmSchema         = cdmSchema,
        syntheaSchema     = syntheaSchema,
        cdmVersion        = cdmVersion,
        syntheaVersion    = syntheaVersion
      ),
      error = function(e) {{
        if (grepl("already exists|duplicate key", e$message, ignore.case=TRUE)) {{
          message("Map & rollup already done; skipping CreateMapAndRollupTables")
        }} else {{
          stop(e)
        }}
      }}
    )

    # Extra indices
    tryCatch(
      ETLSyntheaBuilder::CreateExtraIndices(
        connectionDetails = cd,
        cdmSchema         = cdmSchema,
        syntheaSchema     = syntheaSchema,
        syntheaVersion    = syntheaVersion
      ),
      error = function(e) {{
        if (grepl("already exists", e$message, ignore.case=TRUE)) {{
          message("Indices exist; skipping CreateExtraIndices")
        }} else {{
          stop(e)
        }}
      }}
    )

    # Event tables
    tryCatch(
      ETLSyntheaBuilder::LoadEventTables(
        connectionDetails = cd,
        cdmSchema         = cdmSchema,
        syntheaSchema     = syntheaSchema,
        cdmVersion        = cdmVersion,
        syntheaVersion    = syntheaVersion
      ),
      error = function(e) {{
        if (grepl("already exists|duplicate key", e$message, ignore.case=TRUE)) {{
          message("Events already loaded; skipping LoadEventTables")
        }} else {{
          stop(e)
        }}
      }}
    )
    """
    run_r_script(etl_r_script)


def run_achilles():
    achilles_r_script = f"""
    install.packages("remotes")
    remotes::install_github("OHDSI/Achilles")
    library(Achilles)

    cd <- DatabaseConnector::createConnectionDetails(
      dbms     = "postgresql",
      server   = "localhost/omop",
      user     = "postgres",
      password = "user",
      port     = 5432,
      pathToDriver = "{OMOP_PATH}"
    )

    Achilles::achilles(
      cdmVersion = "5.4", 
      connectionDetails = cd,
      cdmDatabaseSchema = "cdm54",
      resultsDatabaseSchema = "results"
    )
    """
    run_r_script(achilles_r_script)

def run_dqd_checks():
    file_path = os.path.abspath(os.path.join(OUTPUT_PATH, "file.json")).replace("\\", "/")

    dqd_r_script = f"""
    remotes::install_github("OHDSI/DataQualityDashboard", force=TRUE)
    library(DataQualityDashboard)

    cd <- DatabaseConnector::createConnectionDetails(
      dbms     = "postgresql",
      server   = "localhost/omop",
      user     = "postgres",
      password = "user",
      port     = 5432,
      pathToDriver = "{OMOP_PATH}"
    )

    cdmDatabaseSchema = 'omop.cdm54'
    resultsDatabaseSchema='omop.results'
    cdmSourceName='omop.synthea'
    output = '{OUTPUT_PATH}'
    outputfile = file.path(output, 'file.json')  
    print(paste("Output folder:", output))
    print(paste("Output file path:", outputfile))

    print(output)

    executeDqChecks(
      connectionDetails = cd,
      cdmDatabaseSchema = cdmDatabaseSchema,
      resultsDatabaseSchema = resultsDatabaseSchema,
      outputFolder = output,
      cdmSourceName = cdmSourceName,
      cdmVersion = "5.4",
      outputFile = "file.json"
    )

    if (!file.exists(outputfile)) {{
      stop("file.json was not created in the output folder.")
    }}
    viewDqDashboard("{file_path}")
    """
    run_r_script(dqd_r_script)

# === MAIN ===

def main():
    # run_etl_process()
    # run_sql_file(SQL_FILE_PATH)
    # run_achilles()
    # run_sql_file(SQL_FILE_PATH2)
    run_dqd_checks()

if __name__ == "__main__":
    main()
