
# 🌐 Connect to PostgreSQL (Google Cloud SQL) Using R or Terminal

This repository provides a script to connect to a shared **PostgreSQL** database hosted on **Google Cloud SQL**. The database contains raw Synthea-generated healthcare data and is part of the ETL process for mapping to the OMOP Common Data Model (CDM).

---

## 📦 Folder Contents

```
project/
├── csv                     # Synthea Data
├── get_db_connect.R       # R script to connect to Cloud SQL
├── load_synthea.R       # R script to connect to Cloud SQL
├── check.R       # Query checks(modify and play arround)
├── README.md              # This file
```

---

## ✅ 1. Connect to the Database Using R

### 🔹 Step 1: Install Required R Packages

Open R or RStudio and install the required packages:

```r
install.packages(c("DBI", "RPostgres", "readr", "tools"))
```

---

### 🔹 Step 2: Run the Script

Open or source the file `get_db_connect.R`:

```r
source("get_db_connect.R")
```

Contents of `get_db_connect.R`:

```r
library(DBI)
library(RPostgres)
library(readr)
library(tools)

# Connect to PostgreSQL on Google Cloud SQL
con <- dbConnect(
  RPostgres::Postgres(),
  dbname = "omop",
  host = "35.222.151.53",  # Cloud SQL public IP
  port = 5432,
  user = "postgres",
  password = "omop2"
)

# Verify connection
if (dbIsValid(con)) {
  message("✅ Connected to database.")
} else {
  message("❌ Not connected to the database.")
}
```

> ⚠️ IMPORTANT: Your IP address must be whitelisted in the Google Cloud SQL instance.  
> Use https://www.whatismyip.com/ to find your public IP and send it to **Vishnu** to enable access.

---

## 🖥️ 2. Connect Using Terminal (`psql`)

If you prefer using the command line: (contact me to connect with the dp and whitelist your IP)

```bash
psql -h 35.222.151.53 -U postgres -d omop
```

You’ll be prompted for the password:
```
Password: omop2
```

---

## 🧪 3. Test the Connection

After connecting (via R or Terminal), you can run this query to see available tables:

```sql
SELECT tablename FROM pg_tables WHERE schemaname = 'synthea';
```

This will return a list of all tables loaded under the `synthea` schema.

```sql
SELECT * FROM synthea.conditions LIMIT 5;
```

This will return atop 5 items loaded under the `synthea.conditions` schema.

---

## 🧠 Troubleshooting

| Issue                        | Fix                                                   |
|-----------------------------|--------------------------------------------------------|
| ❌ Connection refused        | Ensure your IP is added to **Authorized Networks**     |
| ❌ Password authentication   | Double-check the username and password used            |
| ❌ Cannot resolve host       | Make sure you are online and using the correct IP      |
| ⚠️ Tables not found         | Verify that `synthea` schema exists and data is loaded |

---

## 👤 Maintainer

**Vishnu Priyan**  
Cloud SQL Admin & Data Engineer  
If you're unable to connect or need your IP whitelisted, please reach out.
