import rpy2.robjects as robjects
import psycopg2
import os

# === CONFIGURATION ===
POSTGRES_CONFIG = {
    "dbname": "omop",
    "user": "postgres",
    "password": "omop2",
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
    rlang::last_trace()
    devtools::install_github("OHDSI/ETL-Synthea")
    library(ETLSyntheaBuilder)

    cd <- DatabaseConnector::createConnectionDetails(
      dbms     = "postgresql",
      server   = "localhost/omop",
      user     = "postgres",
      password = "admin123",
      port     = 5432,
      pathToDriver = "{OMOP_PATH}"
    )

    cdmSchema = 'cdm54'
    cdmVersion     <- "5.4"
    syntheaVersion <- "3.3.0"
    syntheaSchema  <- "synthea"
    syntheaFileLoc <- "{CSV_PATH}"
    resultsDatabaseSchema='results'
    cdmSourceName='synthea'

    #create tables for source data
    ETLSyntheaBuilder::CreateSyntheaTables(connectionDetails = cd, syntheaSchema = syntheaSchema, syntheaVersion = syntheaVersion)

    #load data into source data tables
    ETLSyntheaBuilder::LoadSyntheaTables(connectionDetails = cd, syntheaSchema = syntheaSchema, syntheaFileLoc = syntheaFileLoc)

    #create map and rollup tables
    ETLSyntheaBuilder::CreateMapAndRollupTables(connectionDetails = cd, cdmSchema = cdmSchema, syntheaSchema = syntheaSchema, cdmVersion = cdmVersion, syntheaVersion = syntheaVersion)

    ## Optional Step to create extra indices
    ETLSyntheaBuilder::CreateExtraIndices(connectionDetails = cd, cdmSchema = cdmSchema, syntheaSchema = syntheaSchema, syntheaVersion = syntheaVersion)

    #load event tables
    ETLSyntheaBuilder::LoadEventTables(connectionDetails = cd, cdmSchema = cdmSchema, syntheaSchema = syntheaSchema, cdmVersion = cdmVersion, syntheaVersion = syntheaVersion)
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
      password = "admin123",
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
    file_path = os.path.join(OUTPUT_PATH, "file.json").replace("\\", "/")
    dqd_r_script = f"""
    remotes::install_github("OHDSI/DataQualityDashboard", force=TRUE)
    library(DataQualityDashboard)

    cd <- DatabaseConnector::createConnectionDetails(
      dbms     = "postgresql",
      server   = "localhost/omop",
      user     = "postgres",
      password = "admin123",
      port     = 5432,
      pathToDriver = "{OMOP_PATH}"
    )

    cdmDatabaseSchema = 'omop.cdm54'
    resultsDatabaseSchema='omop.results'
    cdmSourceName='omop.synthea'
    outputFolder = '{OUTPUT_PATH}'

    executeDqChecks(cd, cdmDatabaseSchema, resultsDatabaseSchema, 
                    outputFolder, cdmSourceName, cdmVersion="5.4", 
                    outputFile = "file.json")

    viewDqDashboard('{file_path}')
    """
    run_r_script(dqd_r_script)

# === MAIN ===

def main():
    run_etl_process()
    run_sql_file(SQL_FILE_PATH)
    run_achilles()
    run_sql_file(SQL_FILE_PATH2)
    run_dqd_checks()

if __name__ == "__main__":
    main()
