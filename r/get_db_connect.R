library(DBI)
library(RPostgres)
library(readr)
library(tools)

# Create the connection object
con <- dbConnect(
  RPostgres::Postgres(),
  dbname = "omop",
  
  # psql -h 35.222.151.53 -U postgres -d omop
  # Run in Terminal to query, 
  # (send your ip with https://www.whatismyip.com/ so I can enable acces for you IP)
  host = "35.222.151.53", ## To connect Google Cloud DB
  
  # psql -h localhost -U postgres -d omop
  # Run in Terminal to query
  #host = "localhost", ## To connect to Local DB, Use PG Admin
  
  port = 5432,
  user = "postgres",
  password = "omop2"
)

isConnectedtDB <- dbIsValid(con)

if (dbIsValid(con)) {
  message("✅ Connected to database.")
} else {
  message("❌ Not connected to the database.")
}