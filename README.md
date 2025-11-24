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

# Data Engineering Assessment – Solution by Aswanth Lal

## Overview

This exercise evaluates core data-engineering skills:

| Competency | Focus                                                         |
| ---------- | ------------------------------------------------------------- |
| SQL        | Relational modeling, normalization, DDL/DML scripting         |
| Python ETL | Data ingestion, cleaning, transformation, & loading (ELT/ETL) |

---

## 0️. Prerequisites & Setup

### Allowed Technologies

* Python ≥ 3.8 (all ETL/data-processing code)
* MySQL 8 (target relational database)
* Pydantic (data validation)

> All dependencies are listed in `requirements.txt`.

### 1. Clone the Repository

```bash
git clone https://github.com/100x-Home-LLC/data_engineer_assessment.git data_engineer_assessment_aswanth_lal
cd data_engineer_assessment_aswanth_lal
```


### 2. Python Environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/macOS
source venv/bin/activate

pip install -r requirements.txt
```

### 3. Start MySQL via Docker

```bash
docker-compose -f docker-compose.initial.yml up --build -d
```

* Database available at `localhost:3306`
* Credentials as defined in `docker-compose.initial.yml`

---

## 1️. Problem

-provided with a raw JSON file `data/fake_property_data_new.json` containing property records.

* Each row mixes multiple unrelated attributes (property details, HOA data, rehab estimates, valuations, etc.)
* Database is **not normalized** and lacks relational structure
* Use **Field Config.xlsx** in `data/` for business logic mapping

### Task

* Normalize the data into separate relational tables
* Develop a Python ETL script to:

  * Read, clean, transform, and load data into MySQL
  * Apply primary keys and foreign keys to enforce relationships

---

## 2️ Database Schema & Relationships

### Normalized Tables

| Table       | Description                                               | Rows Loaded |
| ----------- | --------------------------------------------------------- | ----------- |
| `property`  | Property details (primary key `id`)                       | 9,788       |
| `leads`     | Lead-related info (`Reviewed_Status`, `Net_Yield`, `IRR`) | 9,788       |
| `valuation` | Property valuations (one-to-many)                         | 24,174      |
| `HOA`       | Homeowner association info (one-to-many)                  | 9,808       |
| `rehab`     | Rehab/renovation estimates (one-to-many)                  | 19,598      |

### Table Relationships (Simplified Diagram)

```
property
 ├─ leads
 ├─ valuation
 ├─ HOA
 └─ rehab
```

> Foreign keys enforce relational integrity (e.g., `valuation.property_id → property.id`)

---

### Field Mapping (Example)

| Source JSON Field          | Destination Table.Column   |
| -------------------------- | -------------------------- |
| `property_title`           | `property.property_title`  |
| `valuation.list_price`     | `valuation.list_price`     |
| `hoa.hoa_amount`           | `HOA.hoa_amount`           |
| `rehab.underwriting_rehab` | `rehab.underwriting_rehab` |
| `leads.net_yield`          | `leads.Net_Yield`          |

> Full mapping follows **Field Config.xlsx**

---

## 3️ ETL Script

### Script Location

`src/etl_load.py`

### Functionality

* Reads `data/fake_property_data_new.json`
* Cleans and transforms nested arrays (`valuation`, `HOA`, `rehab`)
* Normalizes data into separate tables with primary and foreign keys
* Loads data into MySQL

### Run ETL

```bash
python src/etl_load.py
```

> Successfully loads all tables as per row counts above

---



1️ Connect & Query MySQL

Use the following commands to connect and query the database:

# Connect to MySQL
mysql -h 127.0.0.1 -P 3306 -u db_user -p
# Enter the password from credentials


# Show databases
SHOW DATABASES;


# Select your database
USE home_db;


# Show tables
SHOW TABLES;

This ensures that the database is accessible and tables are created correctly.



## 4️ Verification Queries

### Row Counts

```sql
SELECT COUNT(*) FROM property;
SELECT COUNT(*) FROM leads;
SELECT COUNT(*) FROM valuation;
SELECT COUNT(*) FROM HOA;
SELECT COUNT(*) FROM rehab;
```

### Spot-Check Joins

```sql
-- Valuation
SELECT p.property_title, v.list_price
FROM property p
JOIN valuation v ON p.id = v.property_id
LIMIT 10;

-- HOA
SELECT p.property_title, h.hoa_amount, h.hoa_flag
FROM property p
JOIN HOA h ON p.id = h.property_id
LIMIT 10;

-- Rehab
SELECT p.property_title, r.underwriting_rehab, r.rehab_calculation
FROM property p
JOIN rehab r ON p.id = r.property_id
LIMIT 10;
```

---

## 5️ Backup & Restore

### Backup

```bash
docker exec -i <mysql_container_name> \
mysqldump -u <user> -p<password> <database_name> > backup.sql
```

### Restore

```bash
mysql -u <user> -p<password> <database_name> < backup.sql
```

---

## 6️ Dependencies

| Library                 | Purpose                                     |
| ----------------------- | ------------------------------------------- |
| Python ≥ 3.8            | Base language for ETL                       |
| pydantic                | Data validation & type enforcement          |
| mysql-connector-python  | Connect Python → MySQL                      |
| json, decimal, datetime | Standard libraries for parsing & processing |

---

 **Result:**
The ETL pipeline fully normalizes the raw JSON into relational MySQL tables, maintains referential integrity, and passes all verification queries.
