# --- Load Required Packages ---
library(DBI)
library(RPostgres)
library(readr)
library(tools)

source("get_db_connect.R")

# --- Create 'synthea' schema if it doesn't exist ---
dbExecute(con, "CREATE SCHEMA IF NOT EXISTS synthea;")
message("📂 Schema 'synthea' is ready.")

# --- Locate CSV files ---
csv_folder <- "data/csv/"
csv_files <- list.files(csv_folder, pattern = "\\.csv$", full.names = TRUE)

if (length(csv_files) == 0) {
  stop("No CSV files found in ", csv_folder)
}

# --- Load the first CSV file only ---
for (file in csv_files) {
  table_name <- file_path_sans_ext(basename(file))
  message("📥 Loading file: ", file, " → Table: synthea.", table_name)
  
  df <- read_csv(file)
  
  dbWriteTable(
    con,
    name = Id(schema = "synthea", table = table_name),
    value = df,
    overwrite = TRUE,
    row.names = FALSE
  )
  message("✅ Table 'synthea.", table_name, "' created with ", nrow(df), " rows.")
  
  # Save the last table name for use outside loop
  loaded_table <- table_name
  
  #break  # Exit after first file
  }

# --- Post-load check ---
row_count <- dbGetQuery(con, paste0("SELECT COUNT(*) FROM synthea.", loaded_table))
message("Verified in DB: ", row_count[[1]], " rows in 'synthea.", loaded_table, "'.")


# --- List all tables in synthea schema ---
message("📃 Listing all tables in schema 'synthea':")
tables <- dbGetQuery(con, "SELECT tablename FROM pg_tables WHERE schemaname = 'synthea';")
print(tables)

# --- Disconnect ---
dbDisconnect(con)
message("Disconnected from database.")
