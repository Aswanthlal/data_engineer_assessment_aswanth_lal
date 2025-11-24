# Data Engineering Assessment

Welcome!  
This exercise evaluates your core **data-engineering** skills:

| Competency | Focus                                                         |
| ---------- | ------------------------------------------------------------- |
| SQL        | relational modelling, normalisation, DDL/DML scripting        |
| Python ETL | data ingestion, cleaning, transformation, & loading (ELT/ETL) |

---

## 0 Prerequisites & Setup

> **Allowed technologies**

- **Python ≥ 3.8** – all ETL / data-processing code
- **MySQL 8** – the target relational database
- **Pydantic** – For data validation
- List every dependency in **`requirements.txt`** and justify selection of libraries in the submission notes.

---

## 1 Clone the skeleton repo

```
git clone https://github.com/100x-Home-LLC/data_engineer_assessment.git
```

✏️ Note: Rename the repo after cloning and add your full name.

**Start the MySQL database in Docker:**

```
docker-compose -f docker-compose.initial.yml up --build -d
```

- Database is available on `localhost:3306`
- Credentials/configuration are in the Docker Compose file
- **Do not change** database name or credentials

For MySQL Docker image reference:
[MySQL Docker Hub](https://hub.docker.com/_/mysql)

---

### Problem

- You are provided with a raw JSON file containing property records is located in data/
- Each row relates to a property. Each row mixes many unrelated attributes (property details, HOA data, rehab estimates, valuations, etc.).
- There are multiple Columns related to this property.
- The database is not normalized and lacks relational structure.
- Use the supplied Field Config.xlsx (in data/) to understand business semantics.

### Task

- **Normalize the data:**

  - Develop a Python ETL script to read, clean, transform, and load data into your normalized MySQL tables.
  - Refer the field config document for the relation of business logic
  - Use primary keys and foreign keys to properly capture relationships

- **Deliverable:**
  - Write necessary python and sql scripts
  - Place your scripts in `src/`
  - The scripts should take the initial json to your final, normalized schema when executed
  - Clearly document how to run your script, dependencies, and how it integrates with your database.

---

## Submission Guidelines

- Edit the section to the bottom of this README with your solutions and instructions for each section at the bottom.
- Ensure all steps are fully **reproducible** using your documentation
- DO NOT MAKE THE REPOSITORY PUBLIC. ANY CANDIDATE WHO DOES IT WILL BE AUTO REJECTED.
- Create a new private repo and invite the reviewer https://github.com/mantreshjain and https://github.com/siddhuorama

---

**Good luck! We look forward to your submission.**

## Solutions and Instructions (Filed by Candidate)

**Document your solution here:**

# 1️ Environment Setup

## 1. Clone the repository and rename it:


```
git clone https://github.com/100x-Home-LLC/data_engineer_assessment.git data_engineer_assessment_aswanth_lal

cd data_engineer_assessment_aswanth_lal
```

Create a Python virtual environment and install dependencies:

```
python -m venv venv
venv\Scripts\activate   # Windows

### OR

source venv/bin/activate # Linux/macOS

pip install -r requirements.txt

```

Start MySQL via Docker:

```
docker-compose -f docker-compose.initial.yml up --build -d
```

Database available at localhost:3307


Credentials as defined in docker-compose.initial.yml

# Database Schema & Relationships

## Normalized Tables

### Tables and Descriptions

| Table     | Description                                           | Rows Loaded |
|-----------|-------------------------------------------------------|------------|
| `property` | Property details, primary key `id`                  | 9,788      |
| `leads`    | Lead-related info (`Reviewed_Status`, `Net_Yield`, `IRR`) | 9,788      |
| `valuation` | Property valuations (one-to-many)                  | 24,174     |
| `HOA`      | Homeowner association info (one-to-many)           | 9,808      |
| `Rehab`    | Rehab/renovation estimates (one-to-many)           | 19,598     |

---

## Table Relationships (Simplified Diagram)




property
   ├─ leads
   ├─ valuation
   ├─ HOA
   └─ Rehab


Foreign keys enforce relational integrity (e.g., valuation.property_id → property.id).

# ETL Script

## Script Location
`src/etl_load.py`

## Functionality
- Reads data from `data/fake_property_data_new.json`.

- Cleans and transforms nested arrays (`Valuation`, `HOA`, `Rehab`).

- Inserts data into normalized MySQL tables using proper foreign keys.

## Running the Script
```bash
python src/etl_load.py
```

Successfully loaded all tables with the row counts shown above.

## Verification Queries

 Row counts
SELECT COUNT(*) FROM property;
SELECT COUNT(*) FROM leads;
SELECT COUNT(*) FROM valuation;
SELECT COUNT(*) FROM HOA;
SELECT COUNT(*) FROM Rehab;

 Spot-check joins
SELECT p.property_title, v.list_price
FROM property p
JOIN valuation v ON p.id = v.property_id
LIMIT 10;

SELECT select p.property_title, h.hoa_amount,h.hoa_flag from property p
 join hoa h on p.id=h.property_id limit 10;


SELECT p.property_title, r.underwriting_rehab, r.rehab_calculati
on from property p join rehab r on p.id=r.property_id limit 10;

## Backup / Persistence
Backup the database outside Docker:


docker exec -i <mysql_container_name> mysqldump -u <user> -p<password> <database_name> > backup.sql
Restore later:

mysql -u <user> -p<password> <database_name> < backup.sql

## Dependencies
Python ≥ 3.8

pydantic → data validation

mysql-connector-python → Python → MySQL integration

Other standard libraries: json, decimal, datetime