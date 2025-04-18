# List tables in the synthea schema
source("get_db_connect.R")

message("📋 Previewing data from synthea.considitons:")
preview <- dbGetQuery(con, "SELECT * FROM synthea.conditions LIMIT 5;")
print(preview)

preview <- dbGetQuery(con, "SELECT * FROM synthea.allergies LIMIT 5;")
print(preview)
