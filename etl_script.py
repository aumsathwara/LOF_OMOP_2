import os
import sys
import io
import contextlib
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Setup R environment before loading rpy2
# Ensure R_HOME is set to your R installation path
r_home = os.getenv("R_HOME")
if not r_home:
    # Default path for Windows; adjust as needed
    default_r = r"C:\Program Files\R\R-4.3.0"
    if os.path.isdir(default_r):
        r_home = default_r
    else:
        raise EnvironmentError(
            "R_HOME environment variable is not set and default path not found. "
            "Please set R_HOME to your R installation directory."
        )
os.environ["R_HOME"] = r_home
# Add R bin to PATH so rpy2 can find R executables
r_bin = os.path.join(r_home, "bin")
os.environ["PATH"] = r_bin + os.pathsep + os.environ.get("PATH", "")


import rpy2.robjects as robjects
import psycopg2
import os

class OmopPipeline:
    def __init__(self):
        self.postgres_config = {
            "dbname": "omop",
            "user": "postgres",
            "password": "user",
            "host": "localhost",
            "port": "5432"
        }

        self.omop_path = "."
        self.csv_path = "data/csv"
        self.vocab_csv = "vocabulary_csv"
        self.output_path = "output"

        self.sql_file_schema = "SQLs/results_schema.sql"
        self.sql_file_counts = "SQLs/concept_counts.sql"

        self.required_schemas = ["cdm54", "synthea", "results"]


    def run_r_script(self, script_str: str):
      # Fully suppress R console output, messages, and warnings
      with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        try:
            robjects.r(f"options(warn=-1)")  # Disable warnings
            robjects.r(script_str)
            # robjects.r()
        except Exception as e:
            # Propagate only the R error message
            raise Exception(str(e))
        
    def run_sql_file(self, filepath):
        print(f"Running SQL file: {filepath}")
        try:
            with open(filepath, 'r') as f:
                sql_script = f.read()

            conn = psycopg2.connect(**self.postgres_config)
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

    def ensure_schemas_exist(self):
        try:
            conn = psycopg2.connect(**self.postgres_config)
            cursor = conn.cursor()

            for schema in self.required_schemas:
                cursor.execute(f"""
                    SELECT schema_name FROM information_schema.schemata 
                    WHERE schema_name = '{schema}';
                """)
                if not cursor.fetchone():
                    print(f"Schema '{schema}' not found. Creating it...")
                    cursor.execute(f"CREATE SCHEMA {schema};")

            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print("Error checking or creating schemas:", e)

    def run_etl_process(self):
        try:
          logging.info("Running Full ETL in background...")
          script = f"""
          suppressMessages({{
            options(repos = c(CRAN = 'https://cran.case.edu/'))
            devtools::install_github("OHDSI/ETL-Synthea")
            library(ETLSyntheaBuilder)
          }})
          
          cd <- DatabaseConnector::createConnectionDetails(
            dbms         = "postgresql",
            server       = "localhost/omop",
            user         = "postgres",
            password     = "user",
            port         = 5432,
            pathToDriver = "{self.omop_path}"
          )

          cdmSchema      <- "cdm54"
          cdmVersion     <- "5.4"
          syntheaVersion <- "3.3.0"
          syntheaSchema  <- "synthea"
          vocabFileLoc   <- "{self.vocab_csv}"
          syntheaFileLoc <- "{self.csv_path}"

          ETLSyntheaBuilder::CreateCDMTables(cd, cdmSchema, cdmVersion)
          ETLSyntheaBuilder::LoadVocabFromCsv(cd, cdmSchema, vocabFileLoc)
          ETLSyntheaBuilder::CreateSyntheaTables(cd, syntheaSchema, syntheaVersion)
          ETLSyntheaBuilder::LoadSyntheaTables(cd, syntheaSchema, syntheaFileLoc)

          tryCatch(
            ETLSyntheaBuilder::CreateMapAndRollupTables(cd, cdmSchema, syntheaSchema, cdmVersion, syntheaVersion),
            error = function(e) {{
              if (grepl("already exists|duplicate key", e$message, ignore.case=TRUE)) {{
                message("Skipping CreateMapAndRollupTables")
              }} else stop(e)
            }}
          )

          tryCatch(
            ETLSyntheaBuilder::CreateExtraIndices(cd, cdmSchema, syntheaSchema, syntheaVersion),
            error = function(e) {{
              if (grepl("already exists", e$message, ignore.case=TRUE)) {{
                message("Skipping CreateExtraIndices")
              }} else stop(e)
            }}
          )

          tryCatch(
            ETLSyntheaBuilder::LoadEventTables(cd, cdmSchema, syntheaSchema, cdmVersion, syntheaVersion),
            error = function(e) {{
              if (grepl("already exists|duplicate key", e$message, ignore.case=TRUE)) {{
                message("Skipping LoadEventTables")
              }} else stop(e)
            }}
          )
          """
          self.run_r_script(script)
          logging.info("ETL completed successfully.")
        except Exception as e:
          logging.error(f"Error running ETL: {e}")

    def run_achilles(self):
        script = f"""
        suppressMessages({{
          options(repos = c(CRAN = 'https://cran.case.edu/'))
          install.packages("remotes")
          remotes::install_github("OHDSI/Achilles")
          library(Achilles)
        }})

        cd <- DatabaseConnector::createConnectionDetails(
          dbms     = "postgresql",
          server   = "localhost/omop",
          user     = "postgres",
          password = "user",
          port     = 5432,
          pathToDriver = "{self.omop_path}"
        )

        Achilles::achilles(
          cdmVersion = "5.4", 
          connectionDetails = cd,
          cdmDatabaseSchema = "cdm54",
          resultsDatabaseSchema = "results"
        )
        """
        self.run_r_script(script)

    def run_dqd_checks(self):
      try:
        logging.info("Running DQD checks in background...")
        file_path = os.path.abspath(os.path.join(self.output_path, "file.json")).replace("\\", "/")
        script = f"""
        suppressMessages({{
            options(repos = c(CRAN = 'https://cran.case.edu/'))
            install.packages("remotes", quiet = TRUE)
            library(remotes)
            remotes::install_github("OHDSI/DataQualityDashboard", force = TRUE, quiet = TRUE)
            library(DataQualityDashboard)
        }})
        
        cd <- DatabaseConnector::createConnectionDetails(
          dbms     = "postgresql",
          server   = "localhost/omop",
          user     = "postgres",
          password = "user",
          port     = 5432,
          pathToDriver = "{self.omop_path}"
        )

        cdmDatabaseSchema = 'omop.cdm54'
        resultsDatabaseSchema='omop.results'
        cdmSourceName='omop.synthea'
        output = '{self.output_path}'
        outputfile = file.path(output, 'file.json')

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
          stop("file.json was not created.")
        }}
        viewDqDashboard("{file_path}")
        """
        self.run_r_script(script)
        logging.info("DQD checks completed successfully.")
      except Exception as e:
        logging.error(f"Error running DQD checks: {e}")

        
        
    def run_all(self):
        print("✅ Checking and creating required schemas...")
        self.ensure_schemas_exist()

        print("🚀 Running ETL process...")
        self.run_etl_process()

        print("📥 Running results schema SQL...")
        self.run_sql_file(self.sql_file_schema)

        print("📊 Running Achilles...")
        self.run_achilles()

        print("📈 Running concept counts SQL...")
        self.run_sql_file(self.sql_file_counts)

        print("✅ Running DQD checks...")
        self.run_dqd_checks()


def main():
    pipeline = OmopPipeline()
    pipeline.run_all()

if __name__ == "__main__":
    main()
