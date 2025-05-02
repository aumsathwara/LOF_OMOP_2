# LOF_OMOP_2

### Sprint 1 Tasks (needs updation)
- Look at person's table, person's occurance
- Identify Key Fields from OMOP
- Built a Data Flow Diagram 
- Document Them


### Initializing the project:
Please run ```pip install -r requirements.txt``` to setup all the requirements


### Project layout:
`omop_etl_main.py` is the main injection file. `.env.example` is an environmental file, please use the syntax here to create your own `.env` file with your personal secrets. This file will be ignored by git.



# OMOP ETL
This project automates the Extract, Transform, Load (ETL) pipeline for converting Synthea-generated synthetic healthcare data into the OMOP Common Data Model (CDM) using a combination of Python, R, and PostgreSQL.

It includes:

- CDM table creation  
- Vocabulary and source data loading  
- Achilles data characterization  
- Data Quality Dashboard (DQD) checks  

### Project Structure
```
.  
├── run_etl_scripts.py      # Main script to run the ETL and analysis steps  
├── results_schema.sql      # SQL script to create results schema (optional)  
├── concept_counts.sql      # SQL script for custom concept count analysis  
├── requirements.txt        # Python dependencies  
├── install_r_packages.R    # R packages installation script  
├── postgresql-42.7.5.jar   # PostgreSQL JDBC driver  
├── data/  
│   └── csv/                # Path to Synthea CSV files  
├── output/                 # Output folder for reports and results  
```


### Install Python Dependencies  
Install Python packages via pip:  
```
pip install -r requirements.txt
```


### Install R Dependencies  
Run the R script to install all required OHDSI packages:  
```
Rscript install_r_packages.R  
```
Note: This will use remotes::install_github to install ETLSyntheaBuilder, Achilles, and DataQualityDashboard  


### Running the Pipeline  
Edit `run_etl_scripts.py` and uncomment the steps you want to execute:  
```
run_etl_process()  
#run_sql_file(SQL_FILE_PATH)  
#run_achilles()  
#run_sql_file(SQL_FILE_PATH2)  
#run_dqd_checks()
```

Then run:  
```
python run_etl_scripts.py  
```


### Notes  
- Ensure your PostgreSQL server is running and accessible at localhost:5432.
- Adjust file paths (CSV_PATH, VOCAB_CSV, etc.) in `run_etl_scripts.py` if your directory layout differs.
- Output of DQD is saved as file.json inside the output/ folder and opened automatically in a browser using viewDqDashboard().  
