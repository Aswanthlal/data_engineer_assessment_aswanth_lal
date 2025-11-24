# src/etl_load.py
import os
import json
import re
from decimal import Decimal
from typing import List, Optional
from pydantic import BaseModel, validator
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv

load_dotenv()

# DB config via env 
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_NAME = os.getenv("DB_NAME", "home_db")
DB_USER = os.getenv("DB_USER", "db_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "6equj5_db_user")


JSON_PATH = os.getenv("JSON_PATH", "data/fake_property_data_new.json")

# Helpers
def to_int_if_possible(x):
    if x is None:
        return None
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x)
    if isinstance(x, str):
        m = re.search(r"(\d+)", x.replace(",",""))
        if m:
            return int(m.group(1))
    return None

def to_decimal(x):
    if x is None or x == "":
        return None
    try:
        return Decimal(str(x))
    except Exception:
        # strip non-numeric
        s = re.sub(r"[^\d\.\-]", "", str(x))
        try:
            return Decimal(s) if s not in ("", ".") else None
        except Exception:
            return None

def normalize_yes_no(x):
    if x is None:
        return "Unknown"
    s = str(x).strip().lower()
    if s in ("yes","y","true","1"):
        return "Yes"
    if s in ("no","n","false","0"):
        return "No"
    return "Unknown"

# Pydantic models
class ValuationModel(BaseModel):
    List_Price: Optional[Decimal] = None
    Previous_Rent: Optional[Decimal] = None
    Zestimate: Optional[Decimal] = None
    ARV: Optional[Decimal] = None
    Expected_Rent: Optional[Decimal] = None
    Rent_Zestimate: Optional[Decimal] = None
    Low_FMR: Optional[Decimal] = None
    High_FMR: Optional[Decimal] = None
    Redfin_Value: Optional[Decimal] = None

    @validator('*', pre=True)
    def conv_decimal(cls, v):
        return to_decimal(v)

class HOAModel(BaseModel):
    HOA: Optional[Decimal] = None
    HOA_Flag: Optional[str] = None

    @validator('HOA', pre=True)
    def conv_hoa(cls, v):
        return to_decimal(v)

class RehabModel(BaseModel):
    Underwriting_Rehab: Optional[Decimal] = None
    Rehab_Calculation: Optional[Decimal] = None
    Paint: Optional[str] = None
    Flooring_Flag: Optional[str] = None
    Foundation_Flag: Optional[str] = None
    Roof_Flag: Optional[str] = None
    HVAC_Flag: Optional[str] = None
    Kitchen_Flag: Optional[str] = None
    Bathroom_Flag: Optional[str] = None
    Appliances_Flag: Optional[str] = None
    Windows_Flag: Optional[str] = None
    Landscaping_Flag: Optional[str] = None
    Trashout_Flag: Optional[str] = None

    @validator('Underwriting_Rehab', 'Rehab_Calculation', pre=True)
    def conv_dec(cls, v):
        return to_decimal(v)

class PropertyModel(BaseModel):
    Property_Title: Optional[str]
    Address: Optional[str]
    Reviewed_Status: Optional[str]
    Most_Recent_Status: Optional[str]
    Source: Optional[str]
    Market: Optional[str]
    Occupancy: Optional[str]
    Flood: Optional[str]
    Street_Address: Optional[str]
    City: Optional[str]
    State: Optional[str]
    Zip: Optional[str]
    Property_Type: Optional[str]
    Highway: Optional[str]
    Train: Optional[str]
    Tax_Rate: Optional[Decimal]
    SQFT_Basement: Optional[int]
    HTW: Optional[str]
    Pool: Optional[str]
    Commercial: Optional[str]
    Water: Optional[str]
    Sewage: Optional[str]
    Year_Built: Optional[int]
    SQFT_MU: Optional[int]
    SQFT_Total: Optional[int]
    Parking: Optional[str]
    Bed: Optional[int]
    Bath: Optional[int]
    BasementYesNo: Optional[str]
    Layout: Optional[str]
    Net_Yield: Optional[Decimal]
    IRR: Optional[Decimal]
    Rent_Restricted: Optional[str]
    Neighborhood_Rating: Optional[int]
    Latitude: Optional[float]
    Longitude: Optional[float]
    Subdivision: Optional[str]
    Taxes: Optional[Decimal]
    Selling_Reason: Optional[str]
    Seller_Retained_Broker: Optional[str]
    Final_Reviewer: Optional[str]
    School_Average: Optional[Decimal]
    Valuation: Optional[List[ValuationModel]] = []
    HOA: Optional[List[HOAModel]] = []
    Rehab: Optional[List[RehabModel]] = []

    @validator('Tax_Rate','Net_Yield','IRR','Taxes','School_Average', pre=True)
    def decimal_conv(cls, v):
        return to_decimal(v)

    @validator('SQFT_Basement','SQFT_MU','Year_Built','Bed','Bath','SQFT_Total','Neighborhood_Rating', pre=True)
    def int_conv(cls, v):
        # parse strings eg: "5649 sqft"
        if v is None:
            return None
        return to_int_if_possible(v)

    @validator('HTW','Pool','Commercial','BasementYesNo','Rent_Restricted', pre=True)
    def yn(cls,v):
        return normalize_yes_no(v)

def create_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=False
    )

DDL = open(os.path.join(os.path.dirname(__file__), "ddl.sql")).read() if os.path.exists(os.path.join(os.path.dirname(__file__), "ddl.sql")) else """
-- fallback DDL if ddl.sql not present
CREATE TABLE IF NOT EXISTS property (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    property_title VARCHAR(512),
    address VARCHAR(512),
    market VARCHAR(128),
    street_address VARCHAR(256),
    city VARCHAR(128),
    state VARCHAR(8),
    zip VARCHAR(16),
    property_type VARCHAR(64),
    highway VARCHAR(64),
    train VARCHAR(64),
    tax_rate DECIMAL(6,3),
    sqft_basement INT,
    htw ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    pool ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    commercial ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    water VARCHAR(64),
    sewage VARCHAR(64),
    year_built INT,
    sqft_mu INT,
    sqft_total INT,
    parking VARCHAR(64),
    bed INT,
    bath INT,
    basement_yesno ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    layout VARCHAR(128),
    rent_restricted ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    neighborhood_rating INT,
    latitude DOUBLE,
    longitude DOUBLE,
    subdivision VARCHAR(128),
    taxes DECIMAL(12,2),
    school_average DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_property_title_address (property_title(200), address(200))
);

CREATE TABLE IF NOT EXISTS leads (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    property_id BIGINT NOT NULL,
    reviewed_status VARCHAR(64),
    most_recent_status VARCHAR(64),
    source VARCHAR(128),
    occupancy VARCHAR(64),
    net_yield DECIMAL(8,4),
    irr DECIMAL(8,4),
    final_reviewer VARCHAR(128),
    selling_reason VARCHAR(128),
    seller_retained_broker VARCHAR(128),
    FOREIGN KEY (property_id) REFERENCES property(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS valuation (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    property_id BIGINT NOT NULL,
    list_price DECIMAL(14,2),
    previous_rent DECIMAL(10,2),
    zestimate DECIMAL(14,2),
    arv DECIMAL(14,2),
    expected_rent DECIMAL(10,2),
    rent_zestimate DECIMAL(10,2),
    low_fmr DECIMAL(10,2),
    high_fmr DECIMAL(10,2),
    redfin_value DECIMAL(14,2),
    source_note VARCHAR(256),
    FOREIGN KEY (property_id) REFERENCES property(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS hoa (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    property_id BIGINT NOT NULL,
    hoa_amount DECIMAL(12,2),
    hoa_flag ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    FOREIGN KEY (property_id) REFERENCES property(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS rehab (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    property_id BIGINT NOT NULL,
    underwriting_rehab DECIMAL(12,2),
    rehab_calculation DECIMAL(12,2),
    paint VARCHAR(16),
    flooring_flag ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    foundation_flag ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    roof_flag ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    hvac_flag ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    kitchen_flag ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    bathroom_flag ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    appliances_flag ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    windows_flag ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    landscaping_flag ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    trashout_flag ENUM('Yes','No','Unknown') DEFAULT 'Unknown',
    FOREIGN KEY (property_id) REFERENCES property(id) ON DELETE CASCADE
);
"""

def prepare_db_and_tables(conn):
    cur = conn.cursor()
    for stmt in [s.strip() for s in DDL.split(';') if s.strip()]:
        try:
            cur.execute(stmt + ';')
        except Exception as e:
            print("DDL error:", e)
            conn.rollback()
            raise
    conn.commit()
    cur.close()

def execute_with_check(cur, sql, params):
    """Check that the number of %s matches the number of params before executing."""
    expected = sql.count('%s')
    actual = len(params) if isinstance(params, (tuple, list)) else len(params.keys())
    if expected != actual:
        raise ValueError(f"SQL placeholders ({expected}) != number of parameters ({actual})\nSQL: {sql}\nParams: {params}")
    cur.execute(sql, params)

def insert_property(conn, p: PropertyModel):
    cur = conn.cursor()
    sql = """
    INSERT INTO property
    (property_title, address, market, street_address, city, state, zip, property_type,
     highway, train, tax_rate, sqft_basement, htw, pool, commercial, water, sewage,
     year_built, sqft_mu, sqft_total, parking, bed, bath, basement_yesno, layout,
     rent_restricted, neighborhood_rating, latitude, longitude, subdivision, taxes, school_average)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    v = (
        p.Property_Title, p.Address, p.Market, p.Street_Address, p.City, p.State, p.Zip,
        p.Property_Type, p.Highway, p.Train, p.Tax_Rate, p.SQFT_Basement, p.HTW,
        p.Pool, p.Commercial, p.Water, p.Sewage, p.Year_Built, p.SQFT_MU, p.SQFT_Total,
        p.Parking, p.Bed, p.Bath, p.BasementYesNo, p.Layout, p.Rent_Restricted,
        p.Neighborhood_Rating, p.Latitude, p.Longitude, p.Subdivision, p.Taxes, p.School_Average
    )
    execute_with_check(cur, sql, v)
    property_id = cur.lastrowid
    cur.close()
    return property_id

def insert_leads(conn, property_id, p: PropertyModel):
    cur = conn.cursor()
    sql = """
    INSERT INTO leads (property_id, reviewed_status, most_recent_status, source,
                       occupancy, net_yield, irr, final_reviewer, selling_reason, seller_retained_broker)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    v = (property_id, p.Reviewed_Status, p.Most_Recent_Status, p.Source, p.Occupancy,
         p.Net_Yield, p.IRR, p.Final_Reviewer, p.Selling_Reason, p.Seller_Retained_Broker)
    execute_with_check(cur, sql, v)
    cur.close()

def insert_valuations(conn, property_id, val_list):
    if not val_list:
        return
    cur = conn.cursor()
    sql = """
    INSERT INTO valuation (property_id, list_price, previous_rent, zestimate, arv,
                           expected_rent, rent_zestimate, low_fmr, high_fmr, redfin_value)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    for v in val_list:
        params = (
            property_id, v.List_Price, v.Previous_Rent, v.Zestimate, v.ARV,
            v.Expected_Rent, v.Rent_Zestimate, v.Low_FMR, v.High_FMR, v.Redfin_Value
        )
        execute_with_check(cur, sql, params)
    cur.close()

def insert_hoas(conn, property_id, hoa_list):
    if not hoa_list:
        return
    cur = conn.cursor()
    sql = "INSERT INTO hoa (property_id, hoa_amount, hoa_flag) VALUES (%s,%s,%s)"
    for h in hoa_list:
        params = (property_id, h.HOA, normalize_yes_no(h.HOA_Flag))
        execute_with_check(cur, sql, params)
    cur.close()

def insert_rehabs(conn, property_id, rehab_list):
    if not rehab_list:
        return
    cur = conn.cursor()
    sql = """
    INSERT INTO rehab (property_id, underwriting_rehab, rehab_calculation, paint,
                       flooring_flag, foundation_flag, roof_flag, hvac_flag, kitchen_flag,
                       bathroom_flag, appliances_flag, windows_flag, landscaping_flag, trashout_flag)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    for r in rehab_list:
        params = (
            property_id, r.Underwriting_Rehab, r.Rehab_Calculation, r.Paint,
            normalize_yes_no(r.Flooring_Flag), normalize_yes_no(r.Foundation_Flag),
            normalize_yes_no(r.Roof_Flag), normalize_yes_no(r.HVAC_Flag),
            normalize_yes_no(r.Kitchen_Flag), normalize_yes_no(r.Bathroom_Flag),
            normalize_yes_no(r.Appliances_Flag), normalize_yes_no(r.Windows_Flag),
            normalize_yes_no(r.Landscaping_Flag), normalize_yes_no(r.Trashout_Flag)
        )
        execute_with_check(cur, sql, params)
    cur.close()


def main():
    # read JSON
    with open(JSON_PATH, 'r', encoding='utf-8') as fh:
        data = json.load(fh)

    conn = None
    try:
        conn = create_connection()
    except mysql.connector.Error as err:
        print("DB connection error:", err)
        return

    try:
        prepare_db_and_tables(conn)
        inserted = 0
        for rec in data:
            # validate | normalize
            try:
                pm = PropertyModel(**rec)
            except Exception as e:
                print("Validation failed for record:", e)
                continue

            cur = conn.cursor()
            # optional dedupe: check existing by unique property_title+address
            q = "SELECT id FROM property WHERE property_title=%s AND address=%s LIMIT 1"
            cur.execute(q, (pm.Property_Title, pm.Address))
            row = cur.fetchone()
            if row:
                property_id = row[0]
                # optionally skip or update
                print(f"Skipping duplicate property {pm.Property_Title}")
                cur.close()
                continue

            # insert
            property_id = insert_property(conn, pm)
            insert_leads(conn, property_id, pm)
            insert_valuations(conn, property_id, pm.Valuation or [])
            insert_hoas(conn, property_id, pm.HOA or [])
            insert_rehabs(conn, property_id, pm.Rehab or [])

            conn.commit()
            inserted += 1
            print(f"Inserted property id={property_id}")

        print(f"Done. Inserted {inserted} properties.")
    except Exception as e:
        print("ETL error:", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
